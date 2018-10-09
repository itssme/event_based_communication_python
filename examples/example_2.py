from communication import create_message, ConnectionClient, Master
from time import sleep

import logging


class MasterEventHandler:

    def __init__(self):
        self.server = Master(log_level=logging.ERROR)
        self.server.start_gathering_connections()
        self.connection = ConnectionClient(self, log_level=logging.ERROR)
        self.connection.connect("127.0.0.1")
        self.name = "Master"

    def close(self):
        self.connection.close()
        self.server.close()

    # network events
    def new_peer(self, msg):
        print msg + " to " + self.name


class ClientEventHandler:

    def __init__(self, name):
        self.connection = ConnectionClient(self, log_file="connection_client.log", log_level=logging.ERROR)
        self.connection.connect("127.0.0.1")
        self.name = name
        self.connection.send(create_message("new peer connected -> {}".format(name), "new_peer", -1))

    def close(self):
        self.connection.close()

    # network events
    def new_peer(self, msg):
        print msg + " to " + self.name


def main():
    peer_names = ["Alice", "Bob"]
    server = MasterEventHandler()
    sleep(1)
    clients = [ClientEventHandler(peer_names[i]) for i in xrange(0, len(peer_names))]
    sleep(1)
    for client in clients:
        client.close()
    server.close()


if __name__ == '__main__':
    main()
