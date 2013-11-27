import socket
import threading
import queue
import pickle
import collections
import time

# Message type
Message = collections.namedtuple("Message", ['src', 'to'])


class Server(threading.Thread):
    """
    This class is a server that accepts multiple client connections
    """
    class ServerListener(threading.Thread):
        def __init__(self, owner):
            self.owner = owner
            threading.Thread.__init__(self)

        def run(self):
            while not self.owner.abort:
                try:
                    (data, addr) = self.owner.socket.recvfrom(2048)
                    msg = pickle.loads(data)
                    self.owner.queue.put(msg)
                except queue.Full:
                    print('The message queue is full.')
                except socket.timeout:
                    pass

    def __init__(self, port, address='localhost', timeout=2):
        threading.Thread.__init__(self)
        self.port = port
        self.address = address
        self.timeout = timeout
        self.abort = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 200000)
        self.socket.bind((address, port))
        self.socket.settimeout(timeout)
        self.queue = queue.Queue()
        self.listener = Server.ServerListener(self)

    def run(self):
        self.listener.start()
        while not self.abort:
            message = self.wait_for_message()
            if message is not None:
                print('Server at port {port} receiving message {msg}.'.format(port=self.port, msg=message))
            # self.owner.recv_message(message)

    def wait_for_message(self):
        try:
            msg = self.queue.get(True, 3)  # timeout
            return msg
        except queue.Empty:
            print('The message queue is empty.')

    def send_message(self, message):
        data = pickle.dumps(message)
        address = (self.address, message.to)
        self.socket.sendto(data, address)
        # success
        return True

    def do_abort(self):
        self.abort = True

if __name__ == '__main__':
    a = Server(60000)
    b = Server(60001)

    a.start()
    b.start()

    a.send_message(Message('a', b.port))
    b.send_message(Message('b', a.port))

    time.sleep(2)
    a.send_message(Message('a', b.port))