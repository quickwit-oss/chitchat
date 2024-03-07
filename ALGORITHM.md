Chitchat relies on an extension of the Scuttlebutt reconciliation algorithm, to handle:
- Key-Value deletion
- Member leaving the cluster.

For simplification, we will just discuss the replication of a single map on a cluster of n node, one which only one node can write.
(In scuttlebutt, each node has ownership over such a map.)

There are two kinds of update event on this map:
- Set(K, V)
- Deletion(K)

The writer associates an incremental version to all of the updates.

Let's assume the writer node has done V updates and its map has reached a final
state $\mathcal{S}_f$.
We can use the notation $ ({u_{0}}, ..., {u_i}, ..., {u}_{v=V}) $ to describe the sequence of updates, ordered by version.

At any point in time, each node in the cluster has a view of the state map $\mathcal{S}$. This view is not necessarily up-to-date.

Every second or so, a node $N$ will contact a peers $N'$, requesting for updates. The peer will then send back a subset of updates $(u_i, ..., u_j)$.

By design the algorithm attempts to work with a limited budget. The update is truncated to fit a maximum size called the `mtu`. In `chitchat`, the protocol used is UDP, and the default mtu is of around `65KB`

We call such an update a `delta`.

Upon reception of the updates, the node $N$ may apply this update to update its state.
We can write this operation.

$$\mathcal{S}' = \mathcal{S}~ \top \left(u_i, ..., u_j\right)$$

In order to inform the peer about where the updates should start from, the
node will send two parameters describing how much of the state is replicated:

- `max_version`: One big difference with reliable broadcast, is that this `max version` does NOT mean that the node's state reflects all of the events earlier than
`max_version`. Rather, it says that if the node were to receive all of the events within between `max_version` and $V$ then it will end up to date.
The difference is subtle, but comes from an important optimization of scuttlebutt.
If a node ask for an update above $max_version = m$, and that for a given key $k$ there are two updates after $m$, there is no need to send the first one: it will eventually be overridden by the second one.

- `last_gc_version`: Rather than being deleted right away, the algorithm is using a tombstone mechanism. Key entries are kept by marked as deleted. From the client point of view, everything looks like the key really has been deleted. Eventually, in order
to free memory, we garbage collect all of the key values marked for deletion that are older than a few hours. We then keep track of the maximum `last_gc_version`.

At each round of the protocol, a node makes sure to its pair $(last\_gc\_version, max\_version)$ in the lexicographical sense.

We also defined the subsequence of missing updates $(u')$ extracted from $(u_i)$ by keeping only the update matching the predicate:

- $i > max\_version$ if $u_i$ is a set operation

- $i > max(max\_version, last\_gc\_version)$ if $u_i$ is a delete operation.

The key of the algorithm is to maintain the following invariant.
$$ \mathcal{S}~\top (u') = \mathcal{S}$$

In plain words, the protocol attempts to make sure that, if any nodes were to apply
- all insert updates above its `max_version`
- all delete updates above its `max(max_version, last_gc_version)`
it would have managed to fully replicate $S_f$.

Knowing the `max_version` and the `last_gc_version` of a peer, a node will emit a delta that, if applied to such valid state, would maintain the invariant above and
increase $(last\_gc\_version, max\_version)$ lexicographically.

Due to the nature of UDP and to the concurrent handshake, deltas may not arrive in order or be duplicated. For this reason, upon reception of a delta, a node must ensure that the delta can be applied to the current state, without breaking the invariant,
and increasing $(last\_gc\_version, max\_version)$.




