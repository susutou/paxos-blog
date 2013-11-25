import socket
import threading
import queue


class Server(threading.Thread):
    def __init__(self, port, address='localhost', timeout=2):
        threading.Thread.__init__(self)
        self.port = port
        self.address = address
        self.timeout = timeout
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 200000)
        self.socket.bind((address, port))
        self.socket.settimeout(timeout)
        self.queue = queue.Queue()

    def run(self):
        while True:
            pass
