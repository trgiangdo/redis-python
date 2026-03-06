import socket
import threading
import time

from app.resp_parser import bulk_array, bulk_int, bulk_string, decode_resp

HOST = "localhost"
PORT = 6379

BUFFER_SIZE_BYTES = 1024

# Maps key -> (value, expiry_ms) where expiry_ms is None if no expiry
store: dict[str, tuple[str, float | None]] = {}
list_store: dict[str, list[str]] = {}


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
                lst = list_store.setdefault(args[1], [])
                lst.extend(args[2:])
                conn.sendall(bulk_int(len(lst)))
            case "LRANGE":
                lst = list_store.get(args[1], [])
                start, stop = int(args[2]), int(args[3])
                if stop == -1:
                    stop = len(lst)
                conn.sendall(bulk_array(lst[start:stop + 1]))
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
