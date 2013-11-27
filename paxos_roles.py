import collections

BallotNum = collections.namedtuple("BallotNum", ['number', 'uid'])


class Message (object):

    # Message Types
    MSG_PREPARE = 0
    MSG_PROMISE = 1
    MSG_ACCEPT = 2
    MSG_DECIDE = 3

    def __init__(self):
        self.type = None
        self.source = None
        self.destination = None
        self.ballot_num = None
        self.proposal_value = None
        self.accepted_bal = None
        self.accepted_value = None

    def send_prepare(self, source_uid, destination_uid, ballot_num):
        """
        Broadcast a "prepare" message to all acceptors
        """
        self.type = Message.MSG_PREPARE
        self.source = source_uid
        self.destination = destination_uid
        self.ballot_num = ballot_num

    def send_promise(self, source_uid, destination_uid, ballot_num, accepted_bal, accepted_value):
        """
        Send back a promise message to the specified proposer
        """
        self.type = Message.MSG_PROMISE
        self.source = source_uid
        self.destination = destination_uid
        self.ballot_num = ballot_num
        self.accepted_bal = accepted_bal
        self.accepted_value = accepted_value

    def send_accept(self, source_uid, destination_uid, ballot_num, proposal_value):
        """
        Broadcast an "accept" message to all acceptors
        """
        self.type = Message.MSG_ACCEPT
        self.source = source_uid
        self.destination = destination_uid
        self.ballot_num = ballot_num
        self.proposal_value = proposal_value

    def send_decide(self, source_uid, destination_uid, ballot_num, accepted_value):
        """
        Periodically broadcast decision value to all
        """
        self.type = Message.MSG_DECIDE
        self.source = source_uid
        self.destination = destination_uid
        self.ballot_num = ballot_num
        self.accepted_value = accepted_value


class Proposer (object):

    # State variables
    STATE_UNDEFINED = -1
    STATE_PROPOSED = 0

    def __init__(self, proposer_uid, servers = None):
        self.proposer_uid = proposer_uid
        self.servers = servers or []
        # 5 servers
        self.majority_size = 3

        # Local Message Buffer
        self.message = None

        # Proposer's Properties
        self.state = Proposer.STATE_UNDEFINED
        self.proposed_value = None
        self.ballot_num = BallotNum(number=0, uid=0)
        self.max_accepted_bal = BallotNum(number=0, uid=0)
        self.next_proposal_num = 1
        self.promises_received = set()
        self.agree_cnt = 0

    def send_prepare_request(self):
        """
        Send a "prepare" request to all, initiating the PAXOS consensus algorithm
        """
        self.promises_received.clear()
        self.agree_cnt = 0
        self.ballot_num = BallotNum(self.next_proposal_num, self.proposer_uid)
        self.next_proposal_num += 1

        for server in self.servers:
            self.message.send_prepare(self.proposer_uid, server.acceptor_uid, self.ballot_num)

        # Transit to STATE_PROPOSED after proposed
        self.state = Proposer.STATE_PROPOSED

    def receive_prepare_ack(self, message):
        """
        Learn the outcome of all the pending requests with smaller BallotNums
        """

        # Ignore the message if the destination uid doesn't match or the ACK from that process has been received
        if message.destination != self.proposer_uid or message.source in self.promises_received:
            return

        self.promises_received.add(message.source)

        if self.ballot_num.number == message.ballot_num.number and message.ballot_num.uid == message.ballot_num.uid:
            self.agree_cnt += 1
            if self.agree_cnt >= self.majority_size:
                # Update proposed_value to the accepted_value with the largest BallotNum
                if message.accepted_value is None:
                    # Choose any value
                    self.proposed_value = "My_Value"

                elif (message.accepted_bal.number > self.max_accepted_bal.number) or (message.accepted_bal.number == self.max_accepted_bal.number and message.accepted_bal.uid > self.max_accepted_bal.uid):
                    self.max_accepted_bal = message.accepted_bal
                    self.proposed_value = message.accepted_value

                # Send a "accept" request to all
                for server in self.servers:
                    self.message.send_accept(self.proposer_uid, server.acceptor_uid, self.ballot_num, self.proposed_value)
                # Transit to STATE_UNACCEPTED
                self.state = Proposer.STATE_UNDEFINED

    def state_transition(self, message):
        """
        Proposer's State Machine
        """
        if message is None:
            return

        if self.state == Proposer.STATE_UNDEFINED:
            return
        elif self.state == Proposer.STATE_PROPOSED and message.MSG_PROMISE:
            self.receive_prepare_ack(message)


class Acceptor (object):

    # State variables

    STATE_UNDEFINED = -1
    STATE_PROPOSAL_RECEIVED = 0
    STATE_PROPOSAL_UNDECIDED = 1
    STATE_PROPOSAL_DECIDED = 2

    def __init__(self, acceptor_uid, servers = None):
        self.acceptor_uid = acceptor_uid
        self.servers = servers or []
        # Total num of processes is 5
        self.majority_size = 3

        # Local Message Buffer
        self.message = None

        # Acceptor's Properties
        self.state = Proposer.STATE_UNDEFINED
        self.ballot_num = BallotNum(number=0, uid=0)
        self.accepted_value = None
        self.accepted_bal = BallotNum(number=0, uid=0)
        self.promises_received = None
        self.accepted_cnt = set()
        self.has_broadcast_accept_msg = False

    def receive_prepare_request(self, message):
        """
        Receive a "prepare" request message from a Proposer
        """

        if message.destination != self.acceptor_uid:
            return

        if (message.ballot_num.number > self.ballot_num.number) or (message.ballot_num.number == self.ballot_num.number and message.ballot_num.uid >= self.ballot_num.uid):
            # Promise not to accept ballots smaller than current max in the future
            self.ballot_num = message.ballot_num
            self.message.send_promise(self.acceptor_uid, message.source, message.ballot_num, self.accepted_bal, self.accepted_value)
            self.state = Acceptor.STATE_PROPOSAL_RECEIVED

    def receive_accept_request(self, message):
        """
        Receive an "accept" request message from a Proposer
        """

        if message.destination != self.acceptor_uid:
            return

        # if bal >= promised ballot_num
        if (message.ballot_num.number > self.ballot_num.number) or (message.ballot_num.number == self.ballot_num.number and message.ballot_num.uid >= self.ballot_num.uid):
            self.accepted_bal = message.ballot_num
            self.accepted_value = message.proposal_value
            if (message.ballot_num.number != self.accepted_bal.number or message.ballot_num.uid != self.accepted_bal.uid):
                self.accepted_cnt.clear()
            self.accepted_cnt.add(message.source)
            self.state = Acceptor.STATE_PROPOSAL_UNDECIDED

            if self.has_broadcast_accept_msg == False:
                self.has_broadcast_accept_msg = True
                for server in self.servers:
                    self.message.send_accept(self.acceptor_uid, server.acceptor_uid, self.accepted_bal, self.accepted_value)

            # !!! Here assume no failure (t=0), should be modified in reality !!!
            if len(self.accepted_cnt) >= 5:
                for server in self.servers:
                    self.message.send_decide(self.acceptor_uid, server.acceptor_uid, self.accepted_bal, self.accepted_value)
                self.accepted_cnt.clear()
                self.state = Acceptor.STATE_PROPOSAL_DECIDED

    def state_transition(self, message):
        """
        Acceptor's State Machine
        """
        if self.state == Acceptor.STATE_PROPOSAL_DECIDED:
            # !!! Should figure out when stop sending decisions !!!
            for server in self.servers:
                self.message.send_decide(self.acceptor_uid, server.acceptor_uid, self.accepted_bal, self.accepted_value)
            return
        elif message.MSG_DECIDE:
            self.accepted_bal = message.ballot_num
            self.accepted_value = message.proposal_value
            self.state = Acceptor.STATE_PROPOSAL_DECIDED
            return

        if self.state == Acceptor.STATE_UNDEFINED:
            if message.MSG_PREPARE:
                self.receive_prepare_request(message)
        elif self.state == Acceptor.STATE_PROPOSAL_RECEIVED:
            if message.MSG_PREPARE:
                self.receive_prepare_request(message)
            elif message.MSG_ACCEPT:
                self.receive_accept_request(message)
        elif self.state == Acceptor.STATE_PROPOSAL_UNDECIDED:
            if message.MSG_PREPARE:
                self.receive_prepare_request(message)
            if message.MSG_ACCEPT:
                self.receive_accept_request(message)

