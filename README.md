# chitchat

This crate is used at the core of Quickwit for
- cluster membership
- failure detection
- sharing configuration, and extra metadata values

The idea of relying on scuttlebutt reconciliation and phi-accrual detection is borrowed from
Cassandra, itself borrowing it from DynamoDB.

A anti-entropy gossip algorithm called scuttlebutt is in charge of spreading
a common state to all nodes.

This state is actually divided into namespaces associated to each node.
Let's call them node state.

A node can only edit its own node state.

Rather than sending the entire state, the algorithm makes it possibly to
only transfer updates or deltas of the state.
In addition, delta can be partial in order to fit a UDP packet.

All nodes keep updating an heartbeat key,
so that any node should keep receiving updates from about
any live nodes.

Not receiving any update from node for a given amount of time can therefore be
regarded as a sign of failure. Rather than using a hard threshold,
we use phi-accrual detection to dynamically compute a threshold.

# References

- ScuttleButt paper: https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
- Phi Accrual error detection: https://www.researchgate.net/publication/29682135_The_ph_accrual_failure_detector
- Cassandra details:
https://www.youtube.com/watch?v=FuP1Fvrv6ZQ
- https://docs.datastax.com/en/articles/cassandra/cassandrathenandnow.html
- https://github.com/apache/cassandra/blob/f5fb1b0bd32b5dc7da13ec66d43acbdad7fe9dbf/src/java/org/apache/cassandra/gms/Gossiper.java#L1749
