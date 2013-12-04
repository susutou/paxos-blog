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

ProposalID = collections.namedtuple('ProposalID', ['number', 'uid'])

NODE_PORT = 60000
PROPOSER_PORT = NODE_PORT + 1
ACCEPTOR_PORT = NODE_PORT + 2
LEARNER_PORT = NODE_PORT + 3

# 0, 1, 2 => California, 3 => Virginia, 4 => Oregon
# SERVER_ADDRESSES = ['54.219.110.253', '54.215.98.24', '54.219.212.222', '54.205.186.30', '54.202.79.71']
# SERVER_ADDRESSES = ['localhost' for _ in range(5)]
SERVER_ADDRESSES = [
    'ec2-54-215-98-24.us-west-1.compute.amazonaws.com',   # california
    'ec2-54-216-85-75.eu-west-1.compute.amazonaws.com',   # ireland
    'ec2-54-254-95-123.ap-southeast-1.compute.amazonaws.com',   # singapore
    'ec2-54-226-134-238.compute-1.amazonaws.com',            # virginia
    'ec2-54-203-124-14.us-west-2.compute.amazonaws.com'      # oregon
]


# LogEntry = collections.namedtuple('LogEntry', ['uid', 'value', 'is_accepted'])
class LogEntry(object):
    def __init__(self, uid, value, is_accepted):
        self.uid = uid
        self.value = value
        self.is_accepted = is_accepted

    def __eq__(self, other):
        return self.uid == other.uid and self.value == other.value and self.is_accepted == other.is_accepted

    def __str__(self):
        return str('<uid={u}, value={v}>'.format(u=self.uid, v=self.value))


class Messenger(object):
    def __init__(self, owner):
        self.owner = owner

    def send_proposal(self, replica_id, proposal_value):
        """
        Broadcasts an (uid, value) message to all Acceptors
        """
        for i, addr in enumerate(SERVER_ADDRESSES):
            if i != replica_id:
                msg = Message(
                    Message.MSG_ACCEPT, self.owner.uid,
                    (self.owner.server.address, self.owner.server.port), (addr, ACCEPTOR_PORT + i*10),
                    data=(replica_id, proposal_value))
                self.owner.server.send_message(msg)

    def send_accepted(self, proposal_uid, accepted_value):
        """
        Broadcasts an Accepted message to all Learners
        """
        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_DECIDE, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, LEARNER_PORT + i*10),
                data=(proposal_uid, accepted_value))
            self.owner.server.send_message(msg)

    def on_resolution(self, proposal_uid, value, log=None):
        """
        Called when a resolution is reached
        """
        print('###')
        print('Value {v} is accepted by {o}, proposed by {pid}.'.format(v=value, o=self.owner.server.port, pid=proposal_uid))
        print('###')

        time.sleep(2)

        for i, addr in enumerate(SERVER_ADDRESSES):
            msg = Message(
                Message.MSG_STOP, self.owner.uid,
                (self.owner.server.address, self.owner.server.port), (addr, NODE_PORT + i*10), data=(proposal_uid, value, log))
            self.owner.server.send_message(msg)


class Proposer(object):

    def __init__(self, owner, uid, addr, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port, address=addr)

        self.owner = owner
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

    def propose(self):
        """
        Send the proposal (uid, proposed_value) over all acceptors
        """
        self.owner.log.append(LogEntry(self.uid, self.proposed_value, False))
        self.messenger.send_proposal(self.uid, self.proposed_value)

    def recv_message(self, msg):
        print('illegal!')
        #if msg.type == Message.MSG_PROMISE:
        #    self.recv_promise(msg.src, msg.data[0], msg.data[1], msg.data[2])


class Acceptor(object):
    def __init__(self, owner, uid, addr, port):
        self.messenger = Messenger(self)
        self.server = Server(self, port, address=addr)

        self.owner = owner
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
        if msg.type == Message.MSG_ACCEPT:
            self.recv_accept_request(msg.from_uid, msg.data[0], msg.data[1])

    def recv_accept_request(self, from_uid, proposal_uid, value):  # from_uid is same as proposal uid
        """
        Called when an Accept! message is received from a Proposer
        """

        # write log immediately
        self.owner.log.append(LogEntry(proposal_uid, value, False))

        # broadcast for voting
        self.messenger.send_accepted(proposal_uid, value)


class Learner(object):
    class AsynchronousMessenger(threading.Thread):
        def __init__(self, owner, proposal_uid, value, log=None):
            threading.Thread.__init__(self)
            self.owner = owner
            self.proposal_uid = proposal_uid
            self.value = value
            self.log = log

        def run(self):
            time.sleep(3)
            self.owner.proposals = None
            self.owner.acceptors = None
            self.owner.messenger.on_resolution(self.proposal_uid, self.value, self.log)

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

    def recv_accepted(self, from_uid, proposal_uid, accepted_value):
        """
        Called when an Accepted message is received from an acceptor
        """

        #if self.final_value is not None:
        #    return  # already done

        if self.proposals is None:
            self.proposals = dict()
            self.proposals[self.uid] = 1
            self.acceptors = dict()

        if from_uid in self.acceptors:
            if accepted_value in self.acceptors[from_uid]:
                return
            else:
                self.acceptors[from_uid].add(accepted_value)
        else:
            self.acceptors[from_uid] = {accepted_value}

        # self.proposals: map from proposal_uid => (num_votes, accepted_value)
        if proposal_uid in self.proposals:
            self.proposals[proposal_uid] += 1
        else:
            self.proposals[proposal_uid] = 1

        if self.proposals[proposal_uid] == self.quorum_size:
            accepted_value_tuple = LogEntry(proposal_uid, accepted_value, False)
            accepted_value_tuple_true = LogEntry(proposal_uid, accepted_value, True)
            is_found = False
            i = len(self.owner.log) - 1
            while i >= 0:
                if self.owner.log[i] == accepted_value_tuple_true:
                    is_found = True

                if self.owner.log[i] == accepted_value_tuple:
                    self.owner.log[i].is_accepted = True
                    is_found = True
                    break

                i -= 1

            if not is_found:
                accepted_value_tuple.is_accepted = True
                self.owner.log.append(accepted_value_tuple)

            m = Learner.AsynchronousMessenger(self, proposal_uid, accepted_value, self.owner.log)
            m.start()


class Node(threading.Thread):
    class Daemon(threading.Thread):
        def __init__(self, owner):
            threading.Thread.__init__(self)
            self.owner = owner

        def run(self):
            while True:
                if self.owner.abort:
                    continue

                if self.owner.in_propose_time_frame:
                    self.owner.in_propose_time_frame = False

                    #if self.owner.is_last_decided or self.owner.next_post is None:

                    self.owner.is_last_decided = False

                    self.owner.next_post = self.owner.queue.get(True)
                    print('Propose next available value {}.'.format(self.owner.next_post))
                    self.owner.proposer.set_proposal(self.owner.next_post)
                    self.owner.proposer.propose()
                    #elif self.owner.next_post is not None:
                    #    print('Propose old value {}'.format(self.owner.next_post))
                    #    self.owner.proposer.set_proposal(self.owner.next_post)
                    #    self.owner.proposer.propose()

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

        self.proposer = Proposer(self, uid, addr, port + 1)
        self.acceptor = Acceptor(self, uid, addr, port + 2)
        self.learner = Learner(self, uid, addr, port + 3)

        self.stopped_proposal_id = None

        self.lock = threading.Lock()

        self.last_decided_proposer_id = None
        self.is_last_decided = False

        self.in_propose_time_frame = True

        self.daemon = Node.Daemon(self)

    def recv_message(self, msg):
        with self.lock:
            #if msg.type == Message.MSG_STOP and msg.data[0].number != self.stopped_proposal_id:
            if msg.type == Message.MSG_STOP and not self.is_last_decided:
                self.is_last_decided = True
                # set local log
                accepted_log = msg.data[2]
                if len(accepted_log) > len(self.log):
                    self.log = list(accepted_log)

                i = len(self.log) - 1
                while i >= 0:
                    if not self.log[i].is_accepted:
                        del self.log[i]

                    i -= 1

                #print('Log after this round on site {}: '.format(self.uid))
                #print('[', end='')
                #for log_entry in self.log:
                #    print(log_entry, end=', ')
                #
                #print(']\n')

                # self.stopped_proposal_id = msg.data[0].number
                self.proposer.reset()
                self.acceptor.reset()
                self.learner.reset()

                #self.last_decided_proposer_id = msg.data[0]
                #if msg.data[0] == self.uid:

                time.sleep(6)

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
            print('[', end='')
            for log_entry in self.owner.log:
                print(log_entry, end=', ')

            print(']\n')

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
        nodes[0].queue.put('b', True, 1)
        nodes[0].queue.put('c', True, 1)

        nodes[1].queue.put('x', True, 1)
        nodes[1].queue.put('y', True, 1)

        nodes[2].queue.put('u', True, 1)
        nodes[2].queue.put('v', True, 1)
        nodes[2].queue.put('w', True, 1)

        for node in nodes:
            node.start()