#!/usr/bin/env python3

import socket


def main() -> None:
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket, _ = server_socket.accept()
    while True:
        client_socket.recv(1024)
        client_socket.sendall("+PONG\r\n".encode())


if __name__ == "__main__":
    main()
