import socket
import threading
import time

from app.resp_parser import bulk_array, bulk_int, bulk_stream_entries, bulk_string, decode_resp

HOST = "localhost"
PORT = 6379

BUFFER_SIZE_BYTES = 1024

# Maps key -> (value, expiry_ms) where expiry_ms is None if no expiry
store: dict[str, tuple[str, float | None]] = {}
list_store: dict[str, list[str]] = {}
list_condition = threading.Condition()

# Each stream entry: (id, {field: value, ...})
stream_store: dict[str, list[tuple[str, dict[str, str]]]] = {}


_XADD_ID_ERROR = b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
_XADD_ID_ZERO_ERROR = b"-ERR The ID specified in XADD must be greater than 0-0\r\n"


def _generate_stream_id(key: str, requested_id: str) -> tuple[str, bytes | None]:
    """Returns (generated_id, error_bytes). If error_bytes is not None, the ID is invalid."""
    entries = stream_store.get(key, [])
    # Base case: treat empty stream as if last entry was 0-0 (so minimum valid ID is 0-1)
    last_ms, last_seq = (int(entries[-1][0].split("-")[0]), int(entries[-1][0].split("-")[1])) if entries else (0, 0)

    if requested_id == "*":
        ms = max(int(time.time() * 1000), last_ms)
        seq = (last_seq + 1) if ms == last_ms else 0
        return f"{ms}-{seq}", None

    ms_str, seq_str = requested_id.split("-")
    ms = int(ms_str)

    if seq_str == "*":
        if ms < last_ms:
            return "", _XADD_ID_ERROR
        seq = (last_seq + 1) if ms == last_ms else 0
        return f"{ms}-{seq}", None

    seq = int(seq_str)
    if ms == 0 and seq == 0:
        return "", _XADD_ID_ZERO_ERROR
    if (ms, seq) <= (last_ms, last_seq):
        return "", _XADD_ID_ERROR
    return f"{ms}-{seq}", None


def handle_connection(conn: socket.socket) -> None:
    while True:
        data = conn.recv(BUFFER_SIZE_BYTES)
        if not data:
            break
        command = data.decode("utf-8")
        print(f"Received command: {command!r}")

        args = decode_resp(command)
        match args[0].upper():
            case "PING":
                conn.sendall(b"+PONG\r\n")
            case "ECHO":
                conn.sendall(bulk_string(args[1]))
            case "SET":
                expiry_ms = None
                if len(args) >= 4:
                    match args[3].upper():
                        case "EX":
                            expiry_ms = time.time() * 1000 + int(args[4]) * 1000
                        case "PX":
                            expiry_ms = time.time() * 1000 + int(args[4])
                store[args[1]] = (args[2], expiry_ms)
                conn.sendall(b"+OK\r\n")
            case "GET":
                entry = store.get(args[1])
                if entry is None:
                    conn.sendall(b"$-1\r\n")
                else:
                    value, expiry_ms = entry
                    if expiry_ms is not None and time.time() * 1000 > expiry_ms:
                        del store[args[1]]
                        conn.sendall(b"$-1\r\n")
                    else:
                        conn.sendall(bulk_string(value))
            case "RPUSH":
                with list_condition:
                    lst = list_store.setdefault(args[1], [])
                    lst.extend(args[2:])
                    list_condition.notify_all()
                conn.sendall(bulk_int(len(lst)))
            case "LPUSH":
                with list_condition:
                    lst = list_store.setdefault(args[1], [])
                    lst[:0] = reversed(args[2:])
                    list_condition.notify_all()
                conn.sendall(bulk_int(len(lst)))
            case "LRANGE":
                lst = list_store.get(args[1], [])
                start, stop = int(args[2]), int(args[3])
                if stop == -1:
                    stop = len(lst)
                conn.sendall(bulk_array(lst[start:stop + 1]))
            case "LPOP":
                lst = list_store.get(args[1])
                if not lst:
                    conn.sendall(b"$-1\r\n")
                elif len(args) == 2:
                    conn.sendall(bulk_string(lst.pop(0)))
                else:
                    count = int(args[2])
                    popped, lst[:count] = lst[:count], []
                    conn.sendall(bulk_array(popped))
            case "BLPOP":
                try:
                    keys, timeout = args[1:-1], float(args[-1])
                except ValueError:
                    keys, timeout = args[1:], 0.0
                deadline = time.time() + timeout if timeout > 0 else None
                response = None
                with list_condition:
                    while response is None:
                        for key in keys:
                            lst = list_store.get(key)
                            if lst:
                                response = bulk_array([key, lst.pop(0)])
                                break
                        if response:
                            break
                        remaining = max(0.0, deadline - time.time()) if deadline else None
                        if remaining == 0.0:
                            break
                        list_condition.wait(timeout=remaining)
                conn.sendall(response if response else b"*-1\r\n")
            case "XADD":
                key = args[1]
                entry_id, err = _generate_stream_id(key, args[2])
                if err:
                    conn.sendall(err)
                else:
                    fields = dict(zip(args[3::2], args[4::2]))
                    stream_store.setdefault(key, []).append((entry_id, fields))
                    conn.sendall(bulk_string(entry_id))
            case "XRANGE":
                def _parse_id(id_str: str, end: bool = False) -> tuple[float, float]:
                    if id_str == "-":
                        return (0, 0)
                    if id_str == "+":
                        return (float("inf"), float("inf"))
                    if "-" in id_str:
                        ms, seq = id_str.split("-")
                        return (int(ms), int(seq))
                    return (int(id_str), float("inf")) if end else (int(id_str), 0)

                entries = stream_store.get(args[1], [])
                start, end = _parse_id(args[2]), _parse_id(args[3], end=True)
                count = int(args[5]) if len(args) >= 6 and args[4].upper() == "COUNT" else None
                result = [
                    e for e in entries
                    if start <= tuple(int(x) for x in e[0].split("-")) <= end
                ]
                if count is not None:
                    result = result[:count]
                conn.sendall(bulk_stream_entries(result))
            case "TYPE":
                key = args[1]
                if key in store:
                    type_ = "string"
                elif key in list_store:
                    type_ = "list"
                elif key in stream_store:
                    type_ = "stream"
                else:
                    type_ = "none"
                conn.sendall(b"+" + type_.encode() + b"\r\n")
            case "LLEN":
                lst = list_store.get(args[1], [])
                conn.sendall(bulk_int(len(lst)))
            case _:
                conn.sendall(b"-ERR unknown command\r\n")

    conn.close()


def main():
    with socket.create_server((HOST, PORT), reuse_port=True) as server_socket:
        while True:
            connection, _ = server_socket.accept()
            threading.Thread(target=handle_connection, args=(connection,)).start()


if __name__ == "__main__":
    main()
