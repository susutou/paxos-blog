Paxos Blog
==========

The final project for CS271 (Advanced Topics in Distributed Systems) of UCSB,
Fall 2013.


Team
====
@susutou and @sunshine12358


Identical Stream Paxos
======================

Each server has three roles, Proposer, Acceptor and Learner, running parallel.

Each server maintain a FIFO queue of POST() requests, which avoids request lose,guarantees the events that are originated from the same server are totally ordered.

Separate Thread for fetching and proposing requests.

Message passing mechanism (UDP). Each round of consensus starts with MSG_PROPOSE and ends with MSG_STOP.

Log replication for recovered or unsynchronized nodes to learn the past.


Modified Paxos
==============

Once receive a post request, append it to its local log and broadcast an "accepted" message. If a post request is accepted by majority, then commit it globally; otherwise abort. 

Each post shows up on the originated server immediately before any consensus has reached (0 response delay for user). And each round could commit more than one post requests.

Orders are guaranteed through a FIFO queue.

Do phase-1 only when the leader changes.