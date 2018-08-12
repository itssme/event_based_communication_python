"""
Author: itssme
Desc:
   Communication for something
"""

import logging
import socket
import json
from Queue import Empty
from threading import Thread
from multiprocessing import JoinableQueue as Queue
import time
from abc import ABCMeta, abstractmethod

MAIN_PORT = 3000


def create_message(msg, event="", to=0):
    """
    Simple factory for messages

    :param msg: str
                Content of the message
    :param event: str
                  Type of message
    :param to: int
               0..? To one specific peer
               0 is the standard value (Master)
    :return: str
             Returns the message
    """

    message = {"event": event, "content": msg, "to": to}
    message = {"event": "to", "content": message}  # encapsulate message for connection.Master
    return json.dumps(message)


class Connection(Thread):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, event_handler):
        """
        Initializes the connection

        :param event_handler: event_handler of the connection
        """
        super(Connection, self).__init__()
        self.event_handler = event_handler
        self.recv_msg = Queue()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger = logging.getLogger('connection_logger')
        self.thread_running = False

    def get(self, timeout=None):
        """
        Gets all messages not handled by the event handler

        :return: str Message
        """
        return self.recv_msg.get(timeout=timeout)

    def process_message(self, msg_array):
        while msg_array:
            msg = msg_array.pop(0)
            try:
                event = getattr(self.event_handler, msg["event"])
                event(msg["content"])
            except AttributeError:
                self.recv_msg.put(msg)

    def run(self):
        while self.thread_running:
            try:
                msg = self.socket.recv(1024)  # TODO: handle exception: connection reset by peer -> stop connection

                self.logger.info("got message: %s", msg)

                try:
                    msg = json.loads(msg)
                    msg = [msg]
                except ValueError:
                    if msg == "":
                        self.logger.fatal("lost TCP connection to peer -> stopping connection thread")
                        self.socket.close()
                        self.thread_running = False
                    else:
                        self.logger.info("attempting to split messages")

                        # TODO: imrpove with .replace("}{", "}|{").split("|")
                        msg = msg.split("}{")
                        for i in range(0, len(msg)):
                            if msg[i][0] != "{":
                                msg[i] = "{" + msg[i]
                            if msg[i][-1] != "}":
                                msg[i] = msg[i] + "}"

                            try:
                                msg[i] = json.loads(msg[i])
                            except ValueError:
                                self.logger.fatal("still got an invalid messages after split -> \"{]\"".format(msg[i]))
                                del msg[i]

                        self.logger.info("messages after msg extraction -> \"{}\"".format(str(msg)))

                self.process_message(msg)

            except socket.timeout:
                pass  # Ignore timeouts

    def send(self, msg):
        try:
            self.socket.send(msg)
        except Exception as e:
            self.logger.fatal("[!] ERROR COULD NOT SEND MESSAGE -> " + str(e) + " MSG -> " + str(msg))

    def close(self):
        self.logger.info("closing socket")
        self.thread_running = False
        self.socket.close()


class ConnectionClient(Connection):

    def __init__(self, event_handler, log_file="connection.log"):
        super(Connection, self).__init__()
        self.thread_running = False
        self.peer_number = None

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(1)
        self.event_handler = event_handler
        self.recv_msg = Queue()

        self.logger = logging.getLogger(log_file.split(".")[0] if log_file.endswith(".log") else "connection_logger")
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)
        self.logger.info("setup done")

    def connect(self, ip, port=MAIN_PORT):
        connected = False
        while not connected:
            try:
                self.socket.connect((ip, port))
                connected = True
            except Exception as e:
                self.logger.warning("could not connect to master -> " + str(e))
                time.sleep(0.2)

        self.socket.settimeout(10)
        self.peer_number = json.loads(self.socket.recv(1024))["peer"]
        self.socket.settimeout(None)
        self.logger.info("got peer number: " + str(self.peer_number))
        self.thread_running = True
        self.start()


class Master(Thread):

    def __init__(self, port=MAIN_PORT):
        super(Master, self).__init__()

        # PRIVATE
        self.__is_gathering_connections = False
        self.__gather_connections_thread = None

        # PUBLIC
        self.peers = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', port))
        self.socket.listen(2)
        self.socket.settimeout(1)
        self.connections = []
        self.is_running = True

        self.logger = logging.getLogger('master_logger')
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('master.log')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)
        self.logger.info("setup done")

    def start_gathering_connections(self):
        if not self.__is_gathering_connections:
            self.__is_gathering_connections = True
            self.socket.settimeout(1)
            self.__gather_connections_thread = Thread(target=self.__gather_connections).start()

    def stop_gathering_connections(self):
        self.socket.settimeout(None)
        self.__is_gathering_connections = False

    def __gather_connections(self):
        while self.__is_gathering_connections:
            try:
                connection_socket, address = self.socket.accept()
                connection_socket.send(json.dumps({"peer": self.peers}))
                self.peers += 1
                peer = self.__Connection(connection_socket, address, self.logger,
                                         self.PeerEventHandler(self.connections))
                self.connections.append((connection_socket, address, peer))
                peer.start()
                self.logger.info("got connection %s sent peer %i", address, self.peers - 1)
            except socket.timeout:
                pass  # Ignore timeouts

    class __Connection(Connection):
        def __init__(self, peer, address, logger, event_handler):
            Thread.__init__(self)
            self.socket = peer
            self.address = address
            self.logger = logger
            self.event_handler = event_handler
            self.thread_running = True
            self.recv_msg = Queue()

    class PeerEventHandler:
        def __init__(self, peers):
            self.peers = peers

        def to(self, msg):
            to = msg["to"]
            msg.pop("to")
            try:
                self.peers[int(to)][0].send(json.dumps(msg))
            except Exception as e:
                print("[!] could not send message to " + str(to) + " msg -> " + str(msg))

    def close(self):
        self.socket.close()


def main():
    """
    Simple test function
    Defines an example eventHandler.
    As this was initally written for communication with multiple drones the example
    is written with an "drone" object which is just a array for testing.

    :return:
    """

    class EventHandlerTest:
        def __init__(self, drone=None):
            if drone is None:
                drone = []
            self.drone = drone

        def start(self, msg):
            self.drone.append("start")

        def fly_to(self, msg):
            self.drone.append("ok_for {}".format(msg["fly_to"]))

        def land(self, msg):
            self.drone.append("land")

    master = Master()
    event_handler = EventHandlerTest()
    slave = ConnectionClient(event_handler)

    master.start_gathering_connections()
    slave.connect('127.0.0.1')
    time.sleep(0.2)
    master.stop_gathering_connections()
    time.sleep(0.2)

    master.connections[0][0].send(json.dumps({"event": "start", "content": "nix"}))
    master.connections[0][0].send(json.dumps({"event": "no event", "content": "this is random text"}))
    slave.send(create_message("test", "land", 0))
    time.sleep(2)
    print(slave.event_handler.drone)

    print(slave.get(2))
    try:
        print(slave.get(2))
    except Empty:
        print("The Queue is empty, no new message")

    time.sleep(2)
    print("CLOSING SOCKETS")
    slave.close()
    master.close()

    time.sleep(2)
    print("ENDING PROGRAM")


if __name__ == '__main__':
    main()
