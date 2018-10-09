from communication import create_message, ConnectionClient, Master
from time import sleep


class MasterEventHandler:

    def __init__(self):
        self.server = Master()
        self.server.start_gathering_connections()
        self.connection = ConnectionClient(self)
        self.connection.connect("127.0.0.1")

    def close(self):
        self.connection.close()
        self.server.close()

    # network events
    def new_peer(self, msg):
        print msg
        self.connection.send(create_message("hello back from {}".format(self.connection.peer_number), "hello", 1))


class ClientEventHandler:

    def __init__(self):
        self.connection = ConnectionClient(self, log_file="connection_client.log")
        self.connection.connect("127.0.0.1")
        self.send_hello()

    def close(self):
        self.connection.close()

    def send_hello(self):
        self.connection.send(create_message("hello from {}".format(self.connection.peer_number), "new_peer"))

    # network events
    def hello(self, msg):
        print msg


def main():
    server = MasterEventHandler()
    sleep(1)
    client = ClientEventHandler()
    sleep(1)
    client.close()
    server.close()


if __name__ == '__main__':
    main()
