Paxos Blog
==========

The final project for CS271 (Advanced Topics in Distributed Systems) of UCSB,
Fall 2013.


Team Member
===========
Susen Zhao, Guang Yang


Identical Stream Paxos
======================

Each server has three roles, Proposer, Acceptor and Learner, running parallel. A control thread called Node will manage the start, restart and stop of all roles threads. Another separate thread for fetching and proposing requests.

Each server maintain a FIFO queue of POST() requests, which avoids request lose,guarantees the events that are originated from the same server are totally ordered.

Message passing mechanism (UDP). Each round of consensus starts with MSG_PROPOSE and ends with MSG_STOP.

Log replication for recovered or unsynchronized nodes to learn the decided values.

Once a learner received MSG_DECIDE from mojority (3 in this project), it will prepare to decide on the received value. Before commit the decision value to its local log, it will check the replicaed log received from other servers. If the replicated log is longer than its local log, it will copy the missing item sequentially to its local log and then commit the newest decided value to its local log.

The event that learning from replicated log must occurs and only owhen a learner wants to write a new value into its log. Therefore, we a failed node recovers from failure, it won't learn the old decided values immediately until the reach the decided phase of next round.

Due to the fact that paxos protocol doesn't guarantee termination, we use timeout to force one round of paxos reach its end. Once a learner commits a value into its log, it will broadcast the MSG_STOP message and trigger a timer. After time out, the control thread will reset the status of roles threads and wait for a propoer to initiate next round.


Modified Paxos
==============

Improve performance by allowing more than one proposal values are commited in one round.

Proposer will write the proposal value into its local log, then broadcast to all. Once an acceptor receives the the broadcast, it will broadcast MSG_ACCEPTED to all, indicating that it has received the boardcast message. Once a learner received MSG_ACCEPTED for the same proposal from at least 3 distinct processes, this value could be commited to the log at the end of this round; otherwise, the proposal value will be aborted

Once receive a post request, append it to its local log and broadcast an "accepted" message. If a post request is accepted by majority, then commit it globally; otherwise abort. 

Each post shows up on the originated server immediately before any consensus has reached (0 response delay for user). And each round could commit more than one post requests.

Orders are guaranteed through a FIFO queue. Same strategy as ISPaxos.

