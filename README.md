# Paxos Blog

The final project for CS271 (Advanced Topics in Distributed Systems) of UCSB, Fall 2013.


# Team Member
Susen Zhao, Guang Yang


# Identical Stream Paxos (ISPaxos)

Each server has three roles (running simultaneously as different threads)—Proposer, Acceptor and Learner. A control thread called Node will manage the start, restart and termination of all roles threads. There’s also another separate thread for fetching and executing commands issued via command line.

Each server maintains a FIFO queue of `post()` requests, which avoids request to be lost if not chosen in a round, while guaranteeing events originated from the same server are totally ordered.

For, message passing, we are using UDP over socket. Each role maintains a separate thread for receiving messages and put it into a thread-safe queue. During the process, each round of consensus starts with MSG\_PROPOSE and ends with MSG\_STOP.

Each replica also keeps a local log, and log replication is used for recovered or unsynchronized nodes to learn the decided values.

Once a learner received MSG\_DECIDE from a quorum (3 in this project), it will prepare to decide on the received value. Before committing the decision value to its local log, it will check the replicated log received from other servers. If the replicated log is longer than its local log, it will copy the missing item sequentially to its local log and then commit the newest decided value to its local log.

The event learning from replicated log would not start until a learner wants to write a new value into its log. Therefore, when a failed node recovers from failure, it will not learn the old decided values immediately until it reaches the decided phase of next round.

Due to the fact that Paxos protocol does not guarantee termination, we use a timeout to force one round of Paxos to reach its end. Once a learner commits a value into its log, it will broadcast the MSG\_STOP message and trigger a timer. After time out, the control thread will reset the status of all roles threads and wait for a proposer to initiate the next round.


# Modified Paxos

In the modified version, since we only need to ensure that posts issued from the same server are totally ordered, we improve the performance by allowing more than one proposed values to be committed in one round.

Upon receiving the `post()` request for a value, the proposer will immediately write the proposed value into its local log, and then broadcast to all acceptors. Once an acceptor receives the proposal, it will broadcast a MSG\_ACCEPTED to all learners, indicating that it has received the broadcast message. Once a learner received MSG\_ACCEPTED for the same proposal from a quorum, this proposed value could be committed to the log at the end of this round; otherwise, the proposal value will be aborted. We later run a check on previously written value to ensure that every value in the log are accepted by a quorum, and we will remove all rejected values. 

Each post shows up on the originated server immediately before any consensus has reached, thus achieving a less time delay for users using that server. Also, in each round, more than one proposed value can be accepted and thus the performance is largely improved, especially when all posts queue are filled by users. Besides, it also requires less message exchange in this approach, thus gives more performance.

Since we keep all posts in a server in a FIFO queue in their posted order, and in each round only those accepted values are written into the log, while unaccepted values are kept for next rounds (no lost posts), we can ensure a total order for posts issued from the same server. Through the similar voting strategy as in basic Paxos, we are thereby ensuring that each commit are approved by a quorum, that ensuring that there is not lost for these posts. The recovery strategy is somewhat similar to ISPaxos.
