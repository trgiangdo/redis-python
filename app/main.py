import socket
import threading

from app.resp_parser import bulk_string, decode_resp

HOST = "localhost"
PORT = 6379

BUFFER_SIZE_BYTES = 1024

store: dict[str, str] = {}


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
                store[args[1]] = args[2]
                conn.sendall(b"+OK\r\n")
            case "GET":
                value = store.get(args[1])
                conn.sendall(bulk_string(value) if value is not None else b"$-1\r\n")
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
