import socket
import threading


def default_message_handler(server, conn_id, data):
    for connection in server.list_connections():
        if connection.conn_id == conn_id:
            continue

        try:
            connection.send(f"Message from connection with id {conn_id}: {data.decode()}".encode('utf-8'))
        except RuntimeError:
            pass

    print(f"Computed message from connection with id {conn_id}: {data.decode()}")


class ServerTPC:
    INFORM_MESSAGE_SIZE = 10
    MESSAGE_BATCH_SIZE = 2 ** 10
    MESSAGE_MAXIMAL_SIZE = 2 ** 32

    class Connection:
        def __fetch_packet(self, length):
            while self.conn_socket is not None:
                try:
                    return self.conn_socket.recv(length)
                except socket.timeout:
                    continue
                except (OSError, AttributeError):
                    return b''

        def __fetch_message(self):
            length = self.__fetch_packet(ServerTPC.INFORM_MESSAGE_SIZE)
            if length == b'':
                return b''

            length = int.from_bytes(length, byteorder='big')
            message = b''
            for _ in range((length - 1) // ServerTPC.MESSAGE_BATCH_SIZE + 1):
                data = self.__fetch_packet(ServerTPC.MESSAGE_BATCH_SIZE)
                if data == b'':
                    return b''

                message += data

            return message

        def __compute_messages(self):
            while self.conn_socket is not None:
                data = self.__fetch_message()

                if data == b'':
                    if self.conn_socket is not None:
                        self.conn_socket.close()
                        self.conn_socket = None
                    break

                self.server.message_handler(self.server, self.conn_id, data)
            print(f"Closed connection with id {self.conn_id}")

        def __init__(self, server, conn_id, conn_socket):
            self.server = server
            self.conn_id = conn_id
            self.conn_socket = conn_socket

            self.compute_thread = threading.Thread(target=self.__compute_messages)
            self.compute_thread.start()

        def close(self):
            if self.conn_socket is not None:
                self.conn_socket.close()
                self.conn_socket = None
                self.compute_thread.join()

        def is_alive(self):
            return self.compute_thread.is_alive() and self.conn_socket is not None

        def send(self, data: bytes):
            if len(data) > ServerTPC.MESSAGE_MAXIMAL_SIZE:
                raise RuntimeError("Message too large")
            if self.conn_socket is None:
                raise RuntimeError("Connected was closed")
            if data == b'':
                return

            self.conn_socket.send(len(data).to_bytes(ServerTPC.INFORM_MESSAGE_SIZE, 'big'))
            for i in range(0, len(data), ServerTPC.MESSAGE_BATCH_SIZE):
                self.conn_socket.send(data[i:min(i + ServerTPC.MESSAGE_BATCH_SIZE, len(data))])

    def __connections_acceptor(self, read_timeout):
        while self.server_socket is not None:
            try:
                connection, address = self.server_socket.accept()
            except socket.timeout:
                continue
            except (OSError, AttributeError):
                break

            connection.settimeout(read_timeout)
            self.connections[self.connection_id] = self.Connection(self, self.connection_id, connection)
            print(f"Accepted connection from address {address}, attached id: {self.connection_id}")

            self.connection_id += 1
        print(f"Port listening stopped")

    def __init__(self, message_handler=default_message_handler):
        self.message_handler = message_handler
        self.connections = dict()
        self.connection_id = 0

        self.server_socket = None
        self.connections_acceptor_thread = None

    def run(self, port=2000, listen_queue_size=1, listen_timeout=1, read_timeout=1):
        self.stop()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(listen_timeout)
        self.server_socket.bind(('0.0.0.0', port))
        self.server_socket.listen(listen_queue_size)

        self.connections_acceptor_thread = threading.Thread(target=self.__connections_acceptor, args=(read_timeout,))
        self.connections_acceptor_thread.start()
        print(f"Started listening on port {port}")

    def stop(self):
        if self.server_socket is not None:
            self.server_socket.close()
            self.server_socket = None

        if self.connections_acceptor_thread is not None:
            self.connections_acceptor_thread.join()
            self.connections_acceptor_thread = None

        self.connection_id = 0
        for connection in self.connections.values():
            connection.close()
        self.connections.clear()

    def disconnect(self, connection_id):
        connection = self.connections.get(connection_id, None)
        if connection is not None:
            connection.close()

    def get_connection(self, connection_id):
        return self.connections.get(connection_id, None)

    def list_connections(self):
        return list(self.connections.values())


if __name__ == "__main__":
    tpc_server = ServerTPC()
    tpc_server.run()

    print("Enter commands:")
    while True:
        command = input().split()

        if command[0] == "stop":
            tpc_server.stop()
            break

        if command[0] == "disconnect":
            tpc_server.disconnect(int(command[1]))
            continue
