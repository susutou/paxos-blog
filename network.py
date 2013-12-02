import socket
import threading
import queue
import pickle
import collections
import time
import random

# Message type
# Message = collections.namedtuple("Message", ['src', 'to'])


class Message(object):
    # Message Types
    MSG_PREPARE = 0
    MSG_PROMISE = 1
    MSG_ACCEPT = 2
    MSG_DECIDE = 3
    MSG_STOP = 4

    MSG_TYPE = ['MSG_PREPARE', 'MSG_PROMISE', 'MSG_ACCEPT', 'MSG_DECIDE', 'MSG_STOP']

    def __init__(self, message_type, from_uid, src, to, data=None):
        self.type = message_type
        self.from_uid = from_uid
        # src => (address, port), to => (address, port)
        self.src = src
        self.to = to
        self.data = data


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
                    # print('The message queue is full.')
                    pass
                except socket.timeout:
                    pass

    def __init__(self, owner, port, address='localhost', timeout=2):
        threading.Thread.__init__(self)
        self.owner = owner
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
            if message is not None and isinstance(message, Message):
                # print('Server at port {port} receiving message {msg} from {src}.'.format(port=self.port, msg=message, src=message.src))
                # print('Message type: {t}, content: {c}'.format(t=Message.MSG_TYPE[message.type], c=message.data))

                # let the owner keep receiving message and decide what to do
                self.owner.recv_message(message)

    def wait_for_message(self):
        try:
            msg = self.queue.get(True, 3)  # timeout
            return msg
        except queue.Empty:
            # print('The message queue is empty.')
            pass

    def send_message(self, message):
        data = pickle.dumps(message)
        address = message.to
        r = random.Random(time.clock())
        time.sleep(0.01 * r.randrange(0, 10))
        self.socket.sendto(data, address)
        # success
        return True

    def do_abort(self):
        self.abort = True

if __name__ == '__main__':
    a = Server(60000, None)
    b = Server(60001, None)

    a.start()
    b.start()

    a.send_message(Message(Message.MSG_PREPARE, 'a', b.port))
    b.send_message(Message(Message.MSG_PROMISE, 'b', a.port))

    time.sleep(2)
    b.send_message(Message(Message.MSG_ACCEPT, 'b', a.port))