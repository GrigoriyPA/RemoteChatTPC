import socket
import threading


def default_message_handler(client, data):
    print(f"Received message:\n{data.decode()}\n")


class ClientTPC:
    INFORM_MESSAGE_SIZE = 10
    MESSAGE_BATCH_SIZE = 2 ** 10
    MESSAGE_MAXIMAL_SIZE = 2 ** 32

    def __fetch_message(self):
        try:
            length = self.client_socket.recv(ClientTPC.INFORM_MESSAGE_SIZE)
            if length == b'':
                return b''

            length = int.from_bytes(length, byteorder='big')
            message = b''
            for _ in range((length - 1) // ClientTPC.MESSAGE_BATCH_SIZE + 1):
                data = self.client_socket.recv(ClientTPC.MESSAGE_BATCH_SIZE)
                if data == b'':
                    return b''

                message += data

            return message
        except OSError:
            return b''

    def __compute_messages(self):
        while True:
            data = self.__fetch_message()

            if data == b'':
                if self.client_socket is not None:
                    self.client_socket.close()
                    self.client_socket = None
                break

            self.message_handler(self, data)
        print(f"Closed connection to a host")

    def __init__(self, message_handler=default_message_handler):
        self.message_handler = message_handler

        self.client_socket = None
        self.compute_thread = None

    def connect(self, host='127.0.0.1', port=2000):
        self.disconnect()

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((host, port))
        print(f"Connected to host {host} on port {port}")

        self.compute_thread = threading.Thread(target=self.__compute_messages)
        self.compute_thread.start()

    def disconnect(self):
        if self.client_socket is not None:
            self.client_socket.close()
            self.client_socket = None

        if self.compute_thread is not None:
            self.compute_thread.join()
            self.compute_thread = None

    def send(self, data):
        if len(data) > ClientTPC.MESSAGE_MAXIMAL_SIZE:
            raise RuntimeError("Message too large")
        if self.client_socket is None:
            raise RuntimeError("Client is not connected to a host")
        if data == b'':
            return

        self.client_socket.send(len(data).to_bytes(ClientTPC.INFORM_MESSAGE_SIZE, 'big'))
        for i in range(0, len(data), ClientTPC.MESSAGE_BATCH_SIZE):
            self.client_socket.send(data[i:min(i + ClientTPC.MESSAGE_BATCH_SIZE, len(data))])


if __name__ == "__main__":
    remote_host = '127.0.0.1'
    tpc_client = ClientTPC()
    tpc_client.connect(remote_host)

    print("Enter messages:\n")
    while True:
        send_data = input()

        if send_data == "~stop":
            tpc_client.disconnect()
            break

        if send_data == "~connect":
            tpc_client.connect(remote_host)
            continue

        try:
            tpc_client.send(send_data.encode('utf-8'))
            print()
        except RuntimeError as error:
            print(f"Caught error: {error}")
