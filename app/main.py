import socket
import threading

HOST = "localhost"
PORT = 6379

BUFFER_SIZE_BYTES = 1024


def handle_connection(conn: socket.socket) -> None:
    while True:
        data = conn.recv(BUFFER_SIZE_BYTES)
        command = data.decode("utf-8")
        if not command:
            break
        print(f"Received command: {command}")

        conn.sendall(b"+PONG\r\n")

    conn.close()


def main():
    with socket.create_server((HOST, PORT), reuse_port=True) as server_socket:
        while True:
            connection, _ = server_socket.accept()
            threading.Thread(target=handle_connection, args=(connection,)).start()



if __name__ == "__main__":
    main()
