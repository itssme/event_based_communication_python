"""
Author: itssme
Desc:
   Communication for something
"""

import logging
import socket
import json
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
        self.socket.settimeout(1)
        self.logger = logging.getLogger('connection_logger')
        self.thread_running = False

    def get(self, timeout=None):
        """
        Gets all messages not handled by the event handler

        :return: str Message
        """
        return self.recv_msg.get(timeout=timeout)

    def process_message(self, msg_array):
        """
        Takes an array of messages and calls the callback function for each event or puts the msg in the queue if
        there is no corresponding callback function.

        :param msg_array: json
        :return: None
        """
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
                                self.logger.fatal("still got an invalid messages after split -> \"{}\"".format(msg[i]))
                                del msg[i]

                        self.logger.info("messages after msg extraction -> \"{}\"".format(str(msg)))

                self.process_message(msg)

            except socket.timeout:
                pass  # Ignore timeouts

    def send(self, msg):
        """
        Sends a message to the master

        :param msg: str
        :return: None
        """
        try:
            self.socket.send(msg)
        except Exception as e:
            self.logger.fatal("[!] ERROR COULD NOT SEND MESSAGE -> " + str(e) + " MSG -> " + str(msg))

    def close(self):
        """
        Closes socket and stops thread

        :return: None
        """
        self.logger.info("closing socket")
        self.thread_running = False
        self.socket.close()


class ConnectionClient(Connection):

    def __init__(self, event_handler, log_file="connection.log", log_level=logging.DEBUG):
        super(Connection, self).__init__()
        self.thread_running = False
        self.peer_number = None

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(1)
        self.event_handler = event_handler
        self.recv_msg = Queue()

        self.logger = logging.getLogger(log_file.split(".")[0] if log_file.endswith(".log") else "connection_logger")
        self.logger.setLevel(log_level)
        fh = logging.FileHandler(log_file)
        fh.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)
        self.logger.info("setup done")

    def connect(self, ip, port=MAIN_PORT):
        """
        Connect to the master

        :param ip: str
                   ip of the master peer
        :param port: int
                     port of the connection (default: 3000)
        :return: None
        """
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


class Master:

    def __init__(self, port=MAIN_PORT, log_level=logging.DEBUG):
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
        self.logger.setLevel(log_level)
        fh = logging.FileHandler('master.log')
        fh.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)
        self.logger.info("setup done")

    def start_gathering_connections(self):
        """
        Start accepting new connections

        :return: None
        """
        if not self.__is_gathering_connections:
            self.__is_gathering_connections = True
            self.__gather_connections_thread = Thread(target=self.__gather_connections).start()

    def stop_gathering_connections(self):
        """
        Stop accepting new connections

        :return: None
        """
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
            self.socket.settimeout(1)
            self.address = address
            self.logger = logger
            self.event_handler = event_handler
            self.thread_running = True
            self.recv_msg = Queue()

    class PeerEventHandler:
        """"
        Handles redirects from master to other peers
        """

        def __init__(self, peers):
            self.peers = peers

        def to(self, msg):
            """
            Redirects a message

            :param msg: json
            :return: None
            """
            to = msg["to"]
            msg.pop("to")
            try:
                self.peers[int(to)][0].send(json.dumps(msg))
            except Exception as e:
                logging.error("[!] could not send message to " + str(to) + " msg -> " + str(msg))

    def close(self):
        """
        Closes all the connection

        :return: None
        """
        self.stop_gathering_connections()

        self.socket.close()
        for peer in self.connections:
            peer[2].close()
