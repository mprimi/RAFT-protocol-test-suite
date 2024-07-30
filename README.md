# RAFT-protocol-test-suite
## Test suite to guide implementations of RAFT consensus protocol

---

The [RAFT consensus algorithm](https://en.wikipedia.org/wiki/Raft_(algorithm)) was designed to be easier to understand and implement than its older cousin [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)).

While the [paper](https://raft.github.io/raft.pdf) along with plenty of [resources and reference implementations](https://raft.github.io/) can be a great guide, there are still a number of under-specified and ambiguous details that could result in minor and major implementation errors (i.e. deviation from the spec, code bugs, etc.).

Implementing RAFT may be easier than Paxos, but it's still not trivial.
In fact, the well-specified pseudocode may create a false sense of security, leading to implementations that are *confidently wrong*.

This project contains a collection of test specification that cover all the most critical behavior that a RAFT implementation should pass.
