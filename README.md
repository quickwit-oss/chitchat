# Quickwit Scuttlebut

Quickwit Scuttlebut is a rust library that implements the cluster membership gossip protocol used in [Quickwit]([https://](https://github.com/quickwit-inc/quickwit)). It is based on a very efficient anti-entropy Gossip based mechanism defined in the paper [Efficient Reconciliation and Flow Control for Anti-Entropy Protocols](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf).


# More on Scuttlebut

Scuttlebut is notably implemented in the NoSQL distributed database [Cassandra](https://cassandra.apache.org/_/index.html) as stated in the paper [Cassandra - A Decentralized Structured Storage System](https://www.cs.cornell.edu/Projects/ladis2009/papers/lakshman-ladis2009.pdf).
