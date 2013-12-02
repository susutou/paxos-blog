"""
Initial import from essential Paxos
"""
import collections
import pickle
import time
import queue
import threading
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

NODE_PORTS = [60000 + i*4 for i in range(5)]
PROPOSER_PORTS = [i+1 for i in NODE_PORTS]
ACCEPTOR_PORTS = [i+2 for i in NODE_PORTS]
LEARNER_PORTS = [i+3 for i in NODE_PORTS]


class Messenger(object):
    def __init__(self, owner):
        self.owner = owner

    def send_prepare(self, proposal_id):
        """
        Broadcasts a Prepare message to all Acceptors
        """
        for port in ACCEPTOR_PORTS:
            msg = Message(Message.MSG_PREPARE, self.owner.server.port, port, data=proposal_id)
            self.owner.server.send_message(msg)

    def send_promise(self, proposer_uid, proposal_id, previous_id, accepted_value):
        """
        Sends a Promise message to the specified Proposer
        """
        msg = Message(
            Message.MSG_PROMISE, self.owner.server.port, proposer_uid, data=(proposal_id, previous_id, accepted_value))
        self.owner.server.send_message(msg)

    def send_accept(self, proposal_id, proposal_value):
        """
        Broadcasts an Accept! message to all Acceptors
        """
        for port in ACCEPTOR_PORTS:
            msg = Message(Message.MSG_ACCEPT, self.owner.server.port, port, data=(proposal_id, proposal_value))
            self.owner.server.send_message(msg)

    def send_accepted(self, proposal_id, accepted_value):
        """
        Broadcasts an Accepted message to all Learners
        """
        for port in LEARNER_PORTS:
            msg = Message(Message.MSG_DECIDE, self.owner.server.port, port, data=(proposal_id, accepted_value))
            self.owner.server.send_message(msg)

    def on_resolution(self, proposal_id, value):
        """
        Called when a resolution is reached
        """
        print('! Value {v} is accepted by {o}, proposed by {pid}.'.format(v=value, o=self.owner.server.port, pid=proposal_id))
        time.sleep(2)

        for port in NODE_PORTS:
            msg = Message(Message.MSG_STOP, self.owner.server.port, port, data=proposal_id)
            self.owner.server.send_message(msg)


class Proposer(object):

    def __init__(self, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port)

        self.proposer_uid = port
        self.quorum_size = 3

        self.proposed_value = None
        self.proposal_id = None
        self.last_accepted_id = (-1, -1)
        self.next_proposal_number = 1
        self.promises_rcvd = None

    def reset(self):
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
    def __init__(self, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port)

        self.promised_id = ProposalID(-1, -1)
        self.accepted_id = ProposalID(-1, -1)
        self.accepted_value = None

    def reset(self):
        # self.messenger = Messenger(self)
        # self.server = Server(self, port)

        self.promised_id = ProposalID(-1, -1)
        self.accepted_id = ProposalID(-1, -1)
        self.accepted_value = None

    def start(self):
        self.server.start()

    def recv_message(self, msg):
        if msg.type == Message.MSG_PREPARE:
            self.recv_prepare(msg.src, msg.data)  # the data is simply one tuple
        elif msg.type == Message.MSG_ACCEPT:
            self.recv_accept_request(msg.src, msg.data[0], msg.data[1])

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
    def __init__(self, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port)

        self.quorum_size = 3
        self.proposals = None  # maps proposal_id => [accept_count, retain_count, value]
        self.acceptors = None  # maps from_uid => last_accepted_proposal_id
        self.final_value = None
        self.final_proposal_id = None

    def reset(self):
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
    def __init__(self, port, uid):
        threading.Thread.__init__(self)
        self.port = port
        self.server = Server(self, port)
        self.queue = queue.Queue()

        self.uid = uid
        self.next_post = None

        self.proposer = Proposer(port + 1)
        self.acceptor = Acceptor(port + 2)
        self.learner = Learner(port + 3)

        self.stopped_proposal_id = None

    def update_proposal(self):
        try:
            self.next_post = self.queue.get(True, 1)
            self.proposer.set_proposal(self.next_post)
            self.proposer.prepare()
            print('New post {} is updated.'.format(self.next_post))
        except queue.Empty:
            self.next_post = None

    def recv_message(self, msg):
        if msg.type == Message.MSG_STOP and msg.data.number != self.stopped_proposal_id:
            self.stopped_proposal_id = msg.data.number
            self.proposer.reset()
            self.acceptor.reset()
            self.learner.reset()

            proposer_uid = msg.data.uid

            if proposer_uid == self.uid + 1:
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


if __name__ == '__main__':
    #proposers = [Proposer(port) for port in PROPOSER_PORTS]
    #acceptors = [Acceptor(port) for port in ACCEPTOR_PORTS]
    #learners = [Learner(port) for port in LEARNER_PORTS]
    #
    #for proposer in proposers:
    #    proposer.start()
    #
    #for acceptor in acceptors:
    #    acceptor.start()
    #
    #for learner in learners:
    #    learner.start()
    #
    #proposers[0].set_proposal('p1')
    #proposers[1].set_proposal('p2')
    #
    #proposers[0].prepare()
    #proposers[1].prepare()

    nodes = [Node(port, port) for port in NODE_PORTS]

    nodes[0].queue.put('a', True, 1)

    nodes[1].queue.put('b', True, 1)
    nodes[1].queue.put('c', True, 1)

    nodes[0].start()
    nodes[1].start()
    nodes[2].start()
    nodes[3].start()
    nodes[4].start()