import collections
import pickle
import time
import queue
import threading
import sys
import re
from network import Message, Server


ProposalID = collections.namedtuple('ProposalID', ['number', 'uid'])

NODE_PORT = 60000
PROPOSER_PORT = NODE_PORT + 1
ACCEPTOR_PORT = NODE_PORT + 2
LEARNER_PORT = NODE_PORT + 3

# 0, 1, 2 => California, 3 => Virginia, 4 => Oregon
# SERVER_ADDRESSES = ['54.219.110.253', '54.215.98.24', '54.219.212.222', '54.205.186.30', '54.202.79.71']
# SERVER_ADDRESSES = ['localhost' for _ in range(5)]
SERVER_ADDRESSES = [
    'ec2-54-219-49-8.us-west-1.compute.amazonaws.com',   # california
    'ec2-54-220-231-252.eu-west-1.compute.amazonaws.com',   # ireland
    'ec2-122-248-193-242.ap-southeast-1.compute.amazonaws.com',   # singapore
    'ec2-54-204-108-185.compute-1.amazonaws.com',            # virginia
    'ec2-50-112-236-23.us-west-2.compute.amazonaws.com'      # oregon
]


class Messenger(object):
    def __init__(self, owner):
        self.owner = owner

    def send_prepare(self, proposal_id):
        """
        Broadcasts a Prepare message to all Acceptors
        """
        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_PREPARE, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, ACCEPTOR_PORT + i*10),
                data=proposal_id)
            self.owner.server.send_message(msg)

    def send_promise(self, proposer_uid, proposal_id, previous_id, accepted_value):
        """
        Sends a Promise message to the specified Proposer
        """
        msg = Message(
            Message.MSG_PROMISE, self.owner.uid,
            (self.owner.server.address, self.owner.server.port),
            (SERVER_ADDRESSES[proposer_uid], PROPOSER_PORT + proposer_uid*10),
            data=(proposal_id, previous_id, accepted_value))
        self.owner.server.send_message(msg)

    def send_accept(self, proposal_id, proposal_value):
        """
        Broadcasts an Accept! message to all Acceptors
        """
        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_ACCEPT, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, ACCEPTOR_PORT + i*10),
                data=(proposal_id, proposal_value))
            self.owner.server.send_message(msg)

    def send_accepted(self, proposal_id, accepted_value):
        """
        Broadcasts an Accepted message to all Learners
        """
        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_DECIDE, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, LEARNER_PORT + i*10),
                data=(proposal_id, accepted_value))
            self.owner.server.send_message(msg)

    def on_resolution(self, proposal_id, value, log=None):
        """
        Called when a resolution is reached
        """
        print('###')
        print('Value {v} is accepted by {o}, proposed by {pid}.'.format(v=value, o=self.owner.server.port, pid=proposal_id))
        print('###')
        time.sleep(2)

        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_STOP, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, NODE_PORT + i*10), data=(proposal_id, value, log))
            self.owner.server.send_message(msg)


class Proposer(object):

    def __init__(self, uid, addr, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port, address=addr)

        self.uid = uid
        self.proposer_uid = uid
        self.quorum_size = 3

        self.proposed_value = None
        self.proposal_id = None
        self.last_accepted_id = (-1, -1)
        self.next_proposal_number = 1
        self.promises_rcvd = None

    def reset(self):
        self.server.queue = queue.Queue()
        self.proposed_value = None
        self.proposal_id = None
        self.last_accepted_id = (-1, -1)
        # self.next_proposal_number = 1
        self.promises_rcvd = None

    def start(self):
        self.server.start()

    def fail(self):
        self.server.do_abort()

    def recover(self):
        self.reset()
        self.server.recover()

    def set_proposal(self, value):
        """
        Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted.
        """
        if self.proposed_value is None:
            self.proposed_value = value

    def prepare(self):
        """
        Sends a prepare request to all Acceptors as the first step in attempting to
        acquire leadership of the Paxos instance.
        """
        self.promises_rcvd = set()
        self.proposal_id = ProposalID(self.next_proposal_number, self.proposer_uid)

        self.next_proposal_number += 1

        self.messenger.send_prepare(self.proposal_id)

    def recv_message(self, msg):
        if msg.type == Message.MSG_PROMISE:
            self.recv_promise(msg.src, msg.data[0], msg.data[1], msg.data[2])

    def recv_promise(self, from_uid, proposal_id, prev_accepted_id, prev_accepted_value):
        """
        Called when a Promise message is received from an Acceptor
        """

        # Ignore the message if it's for an old proposal or we have already received
        # a response from this Acceptor
        if proposal_id != self.proposal_id or from_uid in self.promises_rcvd:
            return

        self.promises_rcvd.add(from_uid)

        if prev_accepted_id > self.last_accepted_id:
            self.last_accepted_id = prev_accepted_id
            # If the Acceptor has already accepted a value, we MUST set our proposal
            # to that value.
            if prev_accepted_value is not None:
                self.proposed_value = prev_accepted_value

        if len(self.promises_rcvd) == self.quorum_size:
            if self.proposed_value is not None:
                self.messenger.send_accept(self.proposal_id, self.proposed_value)


class Acceptor(object):
    def __init__(self, uid, addr, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port, address=addr)

        self.uid = uid

        self.promised_id = ProposalID(-1, -1)
        self.accepted_id = ProposalID(-1, -1)
        self.accepted_value = None

    def reset(self):
        # self.messenger = Messenger(self)
        # self.server = Server(self, port)
        self.server.queue = queue.Queue()

        self.promised_id = ProposalID(-1, -1)
        self.accepted_id = ProposalID(-1, -1)
        self.accepted_value = None

    def start(self):
        self.server.start()

    def fail(self):
        self.server.do_abort()

    def recover(self):
        self.reset()
        self.server.recover()

    def recv_message(self, msg):
        if msg.type == Message.MSG_PREPARE:
            self.recv_prepare(msg.from_uid, msg.data)  # the data is simply one tuple
        elif msg.type == Message.MSG_ACCEPT:
            self.recv_accept_request(msg.from_uid, msg.data[0], msg.data[1])

    def recv_prepare(self, from_uid, proposal_id):
        """
        Called when a Prepare message is received from a Proposer
        """
        if proposal_id == self.promised_id:
            # Duplicate prepare message
            self.messenger.send_promise(from_uid, proposal_id, self.accepted_id, self.accepted_value)

        elif proposal_id > self.promised_id:
            self.promised_id = proposal_id
            self.messenger.send_promise(from_uid, proposal_id, self.accepted_id, self.accepted_value)

    def recv_accept_request(self, from_uid, proposal_id, value):
        """
        Called when an Accept! message is received from a Proposer
        """
        if proposal_id >= self.promised_id:
            self.promised_id = proposal_id
            self.accepted_id = proposal_id
            self.accepted_value = value
            self.messenger.send_accepted(proposal_id, self.accepted_value)


class Learner(object):
    def __init__(self, owner, uid, addr, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port, address=addr)

        self.owner = owner
        self.uid = uid

        self.quorum_size = 3
        self.proposals = None  # maps proposal_id => [accept_count, retain_count, value]
        self.acceptors = None  # maps from_uid => last_accepted_proposal_id
        self.final_value = None
        self.final_proposal_id = None

    def reset(self):
        self.server.queue = queue.Queue()

        self.proposals = None  # maps proposal_id => [accept_count, retain_count, value]
        self.acceptors = None  # maps from_uid => last_accepted_proposal_id
        self.final_value = None
        self.final_proposal_id = None

    def start(self):
        self.server.start()

    def fail(self):
        self.server.do_abort()

    def recover(self):
        self.reset()
        self.server.recover()

    @property
    def complete(self):
        return self.final_proposal_id is not None

    def recv_message(self, msg):
        if msg.type == Message.MSG_DECIDE:
            self.recv_accepted(msg.src, msg.data[0], msg.data[1])

    def recv_accepted(self, from_uid, proposal_id, accepted_value):
        """
        Called when an Accepted message is received from an acceptor
        """

        if self.final_value is not None:
            return  # already done

        if self.proposals is None:
            self.proposals = dict()
            self.acceptors = dict()

        last_proposal_number = self.acceptors.get(from_uid)

        if last_proposal_number is not None and not proposal_id > last_proposal_number:
            return  # Old message

        self.acceptors[from_uid] = proposal_id

        if last_proposal_number is not None:
            old_proposal = self.proposals[last_proposal_number]
            old_proposal[1] -= 1
            if old_proposal[1] == 0:
                del self.proposals[last_proposal_number]

        if not proposal_id in self.proposals:
            self.proposals[proposal_id] = [0, 0, accepted_value]

        t = self.proposals[proposal_id]

        assert accepted_value == t[2], 'Value mismatch for single proposal!'

        t[0] += 1
        t[1] += 1

        if t[0] == self.quorum_size:
            self.final_value = accepted_value
            self.final_proposal_id = proposal_id
            self.proposals = None
            self.acceptors = None

            self.messenger.on_resolution(proposal_id, accepted_value, self.owner.log)


class Node(threading.Thread):
    class Daemon(threading.Thread):
        def __init__(self, owner):
            threading.Thread.__init__(self)
            self.owner = owner
            self.counter = time.time()

        def run(self):
            while True:
                if self.owner.in_propose_time_frame:
                    if time.time() - self.counter > 2:
                        print('Daemon is alive!')
                        self.counter = time.time()

                    self.owner.in_propose_time_frame = False
                    if self.owner.last_decided_proposer_id == self.owner.uid or self.owner.next_post is None:
                        self.owner.next_post = self.owner.queue.get(True)
                        print('Propose next available value {}.'.format(self.owner.next_post))
                        self.owner.proposer.set_proposal(self.owner.next_post)
                        self.owner.proposer.prepare()
                    elif self.owner.next_post is not None:
                        print('Propose old value {}'.format(self.owner.next_post))
                        self.owner.proposer.set_proposal(self.owner.next_post)
                        self.owner.proposer.prepare()

    def __init__(self, uid, addr, port):
        threading.Thread.__init__(self)
        self.address = addr
        self.port = port
        self.server = Server(self, port, address=addr)
        self.queue = queue.Queue()

        self.abort = False

        self.uid = uid
        self.next_post = None

        # local log of the Node
        self.log = []

        self.proposer = Proposer(uid, addr, port + 1)
        self.acceptor = Acceptor(uid, addr, port + 2)
        self.learner = Learner(self, uid, addr, port + 3)

        self.stopped_proposal_id = None

        self.lock = threading.Lock()

        self.last_decided_proposer_id = None

        self.in_propose_time_frame = True

        self.daemon = Node.Daemon(self)

    def recv_message(self, msg):
        if msg.type == Message.MSG_STOP and msg.data[0] != self.stopped_proposal_id:

            # set local log
            accepted_log = msg.data[2]
            if len(accepted_log) > len(self.log):
                self.log = accepted_log

            if len(self.log) > 0:
                if self.log[-1] != msg.data[1]:
                    self.log.append(msg.data[1])
            else:
                self.log.append(msg.data[1])

            print('Log after this round: ', self.log)

            self.stopped_proposal_id = msg.data[0]
            self.proposer.reset()
            self.acceptor.reset()
            self.learner.reset()

            self.last_decided_proposer_id = msg.data[0].uid

            time.sleep(3)

            self.in_propose_time_frame = True

    def fail(self):
        self.proposer.fail()
        self.acceptor.fail()
        self.learner.fail()

        self.abort = True
        self.server.do_abort()

    def recover(self):
        self.server.recover()
        self.proposer.recover()
        self.acceptor.recover()
        self.learner.recover()

        self.abort = False

    def run(self):
        self.server.start()
        self.proposer.start()
        self.acceptor.start()
        self.learner.start()

        self.daemon.start()


class CLI:
    def __init__(self, owner_node):
        self.owner = owner_node

    def exec(self, command):
        cmd_post = re.match(r'post\((.+)\)', command, flags=re.IGNORECASE)
        cmd_read = re.match(r'read\(\s*\)', command, flags=re.IGNORECASE)
        cmd_fail = re.match(r'fail\(\s*\)', command, flags=re.IGNORECASE)
        cmd_unfail = re.match(r'unfail\(\s*\)', command, flags=re.IGNORECASE)

        if cmd_post:
            post = cmd_post.groups()[0]
            try:
                self.owner.queue.put(post, True, 3)
            except queue.Full:
                pass

        elif cmd_read:
            print('The micro blog posts on this site are:')
            print(self.owner.log)

        elif cmd_fail:
            self.owner.fail()

        elif cmd_unfail:
            old_uid = self.owner.uid
            del self.owner
            self.owner = Node(old_uid, SERVER_ADDRESSES[old_uid], NODE_PORT + old_uid*10)
            self.owner.start()

        else:
            print('Unknown command.')


if __name__ == '__main__':

    if len(sys.argv) >= 2:
        uid = int(sys.argv[1])
        values = sys.argv[2:]

        node = Node(uid, SERVER_ADDRESSES[uid], NODE_PORT + uid*10)
        for v in values:
            node.queue.put(v, True, 1)

        node.start()

        parser = CLI(node)

        line = input('>> ')

        while True:  # quit by entering exit()
            parser.exec(line)
            line = input('>> ')

    else:
        SERVER_ADDRESSES = ['localhost' for _ in range(5)]

        nodes = [Node(i, SERVER_ADDRESSES[i], NODE_PORT + i*10) for i in range(5)]

        nodes[0].queue.put('a', True, 1)

        nodes[1].queue.put('b', True, 1)
        nodes[1].queue.put('c', True, 1)

        for node in nodes:
            node.start()