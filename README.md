# chitchat

This crate is used at the core of Quickwit for
- cluster membership
- failure detection
- sharing configuration, and extra metadata values

The idea of relying on scuttlebutt reconciliation and phi-accrual detection is borrowed from Cassandra, itself borrowing it from DynamoDB.

A anti-entropy gossip algorithm called scuttlebutt is in charge of spreading
a common state to all nodes.

This state is actually divided into namespaces associated to each node.
Let's call them node state.

A node can only edit its own node state.

Rather than sending the entire state, the algorithm makes it possibly to
only transfer updates or deltas of the state.
In addition, delta can be partial in order to fit a UDP packet.

We also abuse `chitchat` in Quickwit and use it like a reliable broadcast,
with different caveats.

# References

- ScuttleButt paper: https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
- Phi Accrual error detection: https://www.researchgate.net/publication/29682135_The_ph_accrual_failure_detector
- Cassandra details:
https://www.youtube.com/watch?v=FuP1Fvrv6ZQ
- https://docs.datastax.com/en/articles/cassandra/cassandrathenandnow.html
- https://github.com/apache/cassandra/blob/f5fb1b0bd32b5dc7da13ec66d43acbdad7fe9dbf/src/java/org/apache/cassandra/gms/Gossiper.java#L1749

# Heartbeat

All nodes keep updating an heartbeat counter, dissmenited in digest messages.
As a result, all nodes keeps receiving updates from about any live nodes.

Not receiving any heartbeat update from a node for a given amount of time can therefore be
regarded as a sign of failure. Rather than using a hard threshold, we use phi-accrual detection to dynamically compute a threshold.
Liveness is a local concept. Every single node computes its own vision of the liveness of all other nodes.

# KV deletion

The deletion of a KV is a just another type of mutation: it is
associated with a version, and replicated using the same mechanism as a KV update.

The library will then interpret this versioned tombstone before exposing kv to
the user.

To avoid keeping deleted KV indefinitely, the library includes a GC mechanism.
Every tombstone is associated with a monotonic timestamp.
It is local in the sense that it is computed locally to the given node, and never shared with other servers.

All KV with a timestamp older than a given `marked_for_deletion_grace_period` will be deleted upon garbage collection. (Note for a given KV, GC can happen at different times on different nodes.)

This yields the following problem: if a node was disconnected for more than `marked_for_deletion_grace_period`, they could have missed the deletion of a KV and never be aware of it.

To address this problem, each node locally keeps a record of the version of the last KV they have GCed (for every single other node).
Here is how it works:

Let's assume a Node A sends a Syn message to a Node B. The digest expresses that A want for updates about Node N with a version stricly greater than `V`.  Node B will compare the version `V` of the digest with its `max_gc_version` for the node N.

If `V > max_gc_version`, Node B knows that no GC has impacted Key values with a version above V. It can safely emit a normal delta to A.

If however V is older, a GC could have been executed. Instead of sending a delta to Node A, Node B will instruct A to reset its state by sending a delta start from version `0`.

Node A, upon reception of the delta, will wipe-off whatever information it has about N, and will start syncing from a blank state.

# Node deletion

In Quickwit, we also use chitchat as a "reliable broadcast with caveats".
The idea of reliable broadcast is that the emission of a message is supposed
to eventually be received by all or none of the correct nodes. Here, a node is called "correct" if it does not fail at any point during its execution.

Of course, if the emitter starts failing before emitting its message, one cannot expect the message to reach anyone.
However, if at least one correct nodes receives the message, it will
eventually reach all correct nodes (assuming the node stays correct).

For this reason, we keep emitting KVs from dead nodes too.

To avoid keeping the state of dead nodes indefinitely, we make
a very important trade off.

If a node is marked as dead for more than `DEAD_NODE_GRACE_PERIOD`, we assume that its state can be safely removed from the system. The grace period is
computed from the last time we received an update from the dead node.

Just deleting the state is of course impossible. After the given `DEAD_NODE_GRACE_PERIOD / 2`, we will mark the dead node as `ScheduledForDeletion`.

We first stop sharing data about nodes in the `ScheduledForDeletion` state,
nor listing them node in our digest.

We also ignore any updates received about the dead node. For simplification, we do not even keep track of the last update received. Eventually, all the nodes of the cluster will have marked the dead node as `ScheduledForDeletion`.

After another `DEAD_NODE_GRACE_PERIOD` / 2 has elapsed since the last update received, we delete the dead node state.

It is important to set `DEAD_NODE_GRACE_PERIOD` with a value such `DEAD_NODE_GRACE_PERIOD / 2` is much greater than the period it takes to detect a faulty node.

Note that we are here breaking the reliable broadcast nature of chitchat.
New nodes joining after `DEAD_NODE_GRACE_PERIOD` for instance, will never know about the state of the dead node.

Also, if a node was disconnected from the cluster for more than `DEAD_NODE_GRACE_PERIOD / 2` and reconnects, it is likely to spread information
about the dead node again. Worse, it could not know about the deletion
of some specific KV and spread them again.

The chitchat library does not include any mechanism to prevent this from happening. They should however eventually get deleted (after a bit more than `DEAD_NODE_GRACE_PERIOD`) if the node is really dead.

If the node is alive, it should be able to fix everyone's state via reset or regular delta.

<!--
Alternative, more concise naming / explanation:

Node deletion

Heartbeats are fed into a phi-accrual detector.
Detector tells live nodes from failed nodes apart.
Failed nodes are GCed after GC_GRACE_PERIOD.
Reliable broadcast

In order to ensure reliable broadcast, we must propagate info about failed nodes for some time shorter than GC_GRACE_PERIOD before deleting them.
To do so, failed nodes are split into two categories: zombie and dead.
First, upon failure, failed nodes become zombie nodes, and we keep sharing data about them.
After ZOMBIE_GRACE_PERIOD, zombie nodes transition to dead nodes, and we stop sharing data about them.
ZOMBIE_GRACE_PERIOD is set to GC_GRACE_PERIOD / 2
-->
