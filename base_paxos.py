"""
Initial import from essential Paxos
"""
import collections
import pickle
import time
import queue
import threading
import sys
import re
from network import Message, Server

# In order for the Paxos algorithm to function, all proposal ids must be
# unique. A simple way to ensure this is to include the proposer's UID
# in the proposal id. This prevents the possibility of two Proposers
# from proposing different values for the same proposal ID.
#
# Python tuples are a simple mechanism that allow the proposal number
# and the UID to be combined easily and in a manner that supports
# comparison. To simplify the code, we'll use "namedtuple" instances
# from the collections module which allows us to write
# "proposal_id.number" instead of "proposal_id[0]".
#
ProposalID = collections.namedtuple('ProposalID', ['number', 'uid'])

NODE_PORT = 60000
PROPOSER_PORT = NODE_PORT + 1
ACCEPTOR_PORT = NODE_PORT + 2
LEARNER_PORT = NODE_PORT + 3

# 0, 1, 2 => California, 3 => Virginia, 4 => Oregon
# SERVER_ADDRESSES = ['54.219.110.253', '54.215.98.24', '54.219.212.222', '54.205.186.30', '54.202.79.71']
# SERVER_ADDRESSES = ['localhost' for _ in range(5)]
SERVER_ADDRESSES = [
    'ec2-54-219-110-253.us-west-1.compute.amazonaws.com',
    'ec2-54-215-98-24.us-west-1.compute.amazonaws.com',
    'ec2-54-219-212-222.us-west-1.compute.amazonaws.com',
    'ec2-54-205-186-30.compute-1.amazonaws.com',
    'ec2-54-202-79-71.us-west-2.compute.amazonaws.com'
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

    def on_resolution(self, proposal_id, value):
        """
        Called when a resolution is reached
        """
        print('###')
        print('Value {v} is accepted by {o}, proposed by {pid}.'.format(v=value, o=self.owner.server.port, pid=proposal_id))
        print('###')
        time.sleep(1)

        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_STOP, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, NODE_PORT + i*10), data=proposal_id)
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
    def __init__(self, uid, addr, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port, address=addr)

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

        last_pn = self.acceptors.get(from_uid)

        if last_pn is not None and not proposal_id > last_pn:
            return  # Old message

        self.acceptors[from_uid] = proposal_id

        if last_pn is not None:
            oldp = self.proposals[last_pn]
            oldp[1] -= 1
            if oldp[1] == 0:
                del self.proposals[last_pn]

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

            self.messenger.on_resolution(proposal_id, accepted_value)


class Node(threading.Thread):
    class CommandListener(threading.Thread):
        def __init__(self, owner):
            self.owner = owner

        def run(self):
            while True:
                with self.owner.lock:
                    if self.owner.last_decided_proposer_id == self.owner.uid:
                        pass

    def __init__(self, uid, addr, port):
        threading.Thread.__init__(self)
        self.address = addr
        self.port = port
        self.server = Server(self, port, address=addr)
        self.queue = queue.Queue()

        self.uid = uid
        self.next_post = None

        self.proposer = Proposer(uid, addr, port + 1)
        self.acceptor = Acceptor(uid, addr, port + 2)
        self.learner = Learner(uid, addr, port + 3)

        self.stopped_proposal_id = None

        self.lock = threading.Lock()

        self.last_decided_proposer_id = None

    def update_proposal(self):
        try:
            self.next_post = self.queue.get(True)
            print('Propose next available value {}.'.format(self.next_post))
            self.proposer.set_proposal(self.next_post)
            self.proposer.prepare()
        except queue.Empty:
            self.next_post = None

    def recv_message(self, msg):
        with self.lock:
            if msg.type == Message.MSG_STOP and msg.data.number != self.stopped_proposal_id:
                self.stopped_proposal_id = msg.data.number
                self.proposer.reset()
                self.acceptor.reset()
                self.learner.reset()

                # self.last_decided_proposer_id = msg.data.uid

                time.sleep(3)

                proposer_uid = msg.data.uid

                if proposer_uid == self.uid:
                    self.update_proposal()
                elif self.next_post is not None:
                    print('Propose old value {}'.format(self.next_post))
                    self.proposer.set_proposal(self.next_post)
                    self.proposer.prepare()

    def run(self):
        self.server.start()
        self.proposer.start()
        self.acceptor.start()
        self.learner.start()

        self.update_proposal()


class Parser(threading.Thread):
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
            pass

        elif cmd_fail:
            pass

        elif cmd_unfail:
            pass

        else:
            print('Unknown command.')

    def run(self):
        line = input('>> ')

        while True:  # quit by entering exit()
            parser.exec(line)
            line = input('>> ')

if __name__ == '__main__':

    if len(sys.argv) >= 2:
        uid = int(sys.argv[1])
        values = sys.argv[2:]

        node = Node(uid, SERVER_ADDRESSES[uid], NODE_PORT + uid*10)
        for v in values:
            node.queue.put(v, True, 1)

        node.start()

        parser = Parser(node)
        parser.start()

    else:
        SERVER_ADDRESSES = ['localhost' for _ in range(5)]

        nodes = [Node(i, SERVER_ADDRESSES[i], NODE_PORT + i*10) for i in range(5)]

        nodes[0].queue.put('a', True, 1)

        nodes[1].queue.put('b', True, 1)
        nodes[1].queue.put('c', True, 1)

        for node in nodes:
            node.start()