import argparse
import socket
import threading
import time

from app.resp_parser import bulk_array, bulk_int, bulk_stream_entries, bulk_string, bulk_xread_response, decode_resp

HOST = "localhost"
DEFAULT_PORT = 6379

BUFFER_SIZE_BYTES = 1024

role = "master"
master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
master_repl_offset = 0

# Maps key -> (value, expiry_ms) where expiry_ms is None if no expiry
store: dict[str, tuple[str, float | None]] = {}
list_store: dict[str, list[str]] = {}
list_condition = threading.Condition()

# Each stream entry: (id, {field: value, ...})
stream_store: dict[str, list[tuple[str, dict[str, str]]]] = {}
stream_condition = threading.Condition()


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


def _execute(args: list[str]) -> bytes:
    cmd = args[0].upper()
    match cmd:
        case "PING":
            return b"+PONG\r\n"
        case "ECHO":
            return bulk_string(args[1])
        case "INFO":
            info = f"role:{role}\r\nmaster_replid:{master_replid}\r\nmaster_repl_offset:{master_repl_offset}"
            return bulk_string(info)
        case "SET":
            expiry_ms = None
            if len(args) >= 4:
                match args[3].upper():
                    case "EX":
                        expiry_ms = time.time() * 1000 + int(args[4]) * 1000
                    case "PX":
                        expiry_ms = time.time() * 1000 + int(args[4])
            store[args[1]] = (args[2], expiry_ms)
            return b"+OK\r\n"
        case "GET":
            entry = store.get(args[1])
            if entry is None:
                return b"$-1\r\n"
            value, expiry_ms = entry
            if expiry_ms is not None and time.time() * 1000 > expiry_ms:
                del store[args[1]]
                return b"$-1\r\n"
            return bulk_string(value)
        case "RPUSH":
            with list_condition:
                lst = list_store.setdefault(args[1], [])
                lst.extend(args[2:])
                list_condition.notify_all()
            return bulk_int(len(lst))
        case "LPUSH":
            with list_condition:
                lst = list_store.setdefault(args[1], [])
                lst[:0] = reversed(args[2:])
                list_condition.notify_all()
            return bulk_int(len(lst))
        case "LRANGE":
            lst = list_store.get(args[1], [])
            start, stop = int(args[2]), int(args[3])
            if stop == -1:
                stop = len(lst)
            return bulk_array(lst[start:stop + 1])
        case "LPOP":
            lst = list_store.get(args[1])
            if not lst:
                return b"$-1\r\n"
            if len(args) == 2:
                return bulk_string(lst.pop(0))
            count = int(args[2])
            popped, lst[:count] = lst[:count], []
            return bulk_array(popped)
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
            return response if response else b"*-1\r\n"
        case "XADD":
            key = args[1]
            entry_id, err = _generate_stream_id(key, args[2])
            if err:
                return err
            fields = dict(zip(args[3::2], args[4::2]))
            with stream_condition:
                stream_store.setdefault(key, []).append((entry_id, fields))
                stream_condition.notify_all()
            return bulk_string(entry_id)
        case "XREAD":
            i = 1
            count = None
            if args[i].upper() == "COUNT":
                count = int(args[i + 1])
                i += 2
            block_ms = None
            if args[i].upper() == "BLOCK":
                block_ms = int(args[i + 1])
                i += 2
            i += 1  # skip STREAMS
            remaining = args[i:]
            mid = len(remaining) // 2
            keys, start_ids = remaining[:mid], remaining[mid:]

            def _parse_exclusive_id(id_str: str, key: str) -> tuple[int, int]:
                if id_str == "$":
                    entries = stream_store.get(key, [])
                    if entries:
                        ms, seq = entries[-1][0].split("-")
                        return (int(ms), int(seq))
                    return (0, 0)
                ms, seq = id_str.split("-")
                return (int(ms), int(seq))

            def _read_streams() -> list | None:
                result = []
                for key, start_id_str in zip(keys, start_ids):
                    after = _parse_exclusive_id(start_id_str, key)
                    entries = [
                        e for e in stream_store.get(key, [])
                        if (lambda p: (int(p[0]), int(p[1])))(e[0].split("-")) > after
                    ]
                    if count is not None:
                        entries = entries[:count]
                    if entries:
                        result.append((key, entries))
                return result if result else None

            deadline = time.time() + block_ms / 1000 if block_ms is not None and block_ms > 0 else None
            with stream_condition:
                resolved_ids = [_parse_exclusive_id(sid, k) for k, sid in zip(keys, start_ids)]
                start_ids = [f"{ms}-{seq}" for ms, seq in resolved_ids]

                response = _read_streams()
                while response is None and block_ms is not None:
                    remaining_time = max(0.0, deadline - time.time()) if deadline else None
                    if remaining_time == 0.0:
                        break
                    stream_condition.wait(timeout=remaining_time)
                    response = _read_streams()

            return bulk_xread_response(response) if response else b"*-1\r\n"
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
                if start <= (lambda p: (int(p[0]), int(p[1])))(e[0].split("-")) <= end
            ]
            if count is not None:
                result = result[:count]
            return bulk_stream_entries(result)
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
            return b"+" + type_.encode() + b"\r\n"
        case "INCR":
            entry = store.get(args[1])
            current = entry[0] if entry is not None else "0"
            try:
                new_val = int(current) + 1
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            store[args[1]] = (str(new_val), entry[1] if entry else None)
            return bulk_int(new_val)
        case "LLEN":
            lst = list_store.get(args[1], [])
            return bulk_int(len(lst))
        case _:
            return b"-ERR unknown command\r\n"


def handle_connection(conn: socket.socket) -> None:
    in_multi = False
    queue: list[list[str]] = []

    while True:
        data = conn.recv(BUFFER_SIZE_BYTES)
        if not data:
            break
        command = data.decode("utf-8")
        print(f"Received command: {command!r}")

        args = decode_resp(command)
        cmd = args[0].upper()

        if cmd == "MULTI":
            in_multi = True
            conn.sendall(b"+OK\r\n")
            continue

        if cmd == "EXEC":
            if not in_multi:
                conn.sendall(b"-ERR EXEC without MULTI\r\n")
                continue
            responses = [_execute(queued_args) for queued_args in queue]
            header = b"*" + str(len(responses)).encode() + b"\r\n"
            conn.sendall(header + b"".join(responses))
            in_multi = False
            queue = []
            continue

        if cmd == "DISCARD":
            if not in_multi:
                conn.sendall(b"-ERR DISCARD without MULTI\r\n")
                continue
            in_multi = False
            queue = []
            conn.sendall(b"+OK\r\n")
            continue

        if in_multi:
            queue.append(args)
            conn.sendall(b"+QUEUED\r\n")
            continue

        conn.sendall(_execute(args))

    conn.close()


def main():
    global role
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--replicaof", type=str, default=None)
    args = parser.parse_args()

    if args.replicaof:
        role = "slave"

    with socket.create_server((HOST, args.port), reuse_port=True) as server_socket:
        while True:
            connection, _ = server_socket.accept()
            threading.Thread(target=handle_connection, args=(connection,)).start()


if __name__ == "__main__":
    main()
