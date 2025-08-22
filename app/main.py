import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client
    connection, addr = server_socket.accept()
    connection.sendall(b"+PONG\r\n")
    # with socket.create_connection(("localhost", 6379)) as server:
    #     client, addr  = server.accept()
    #     client.sendall(b"+PONG\r\n")
    # with socket.creat

if __name__ == "__main__":
    main()
