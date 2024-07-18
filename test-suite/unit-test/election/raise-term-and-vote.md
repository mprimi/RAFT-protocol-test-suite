# Raise Term and vote with empty log


## Initial state
```
nodeId: "A"
currentTerm: N
votedFor: nil
log: [] (lastLogIndex: 0)
commitIndex: 0
lastApplied: 0
```

## Vote request:
```
{
  term: T
  candidateId: "B"
  lastLogIndex: 0
  lastLogTerm: 0 (or nil, since lastLogIndex is not a real index)
}
```

## State after vote request processed:
```
nodeId: "A"
currentTerm: T (persisted)
votedFor: "B" (persisted)
log: [] (lastLogIndex: 0)
commitIndex: 0
lastApplied: 0
```

## Vote response:
```
{
  term: T
  voteGranted: true
  voterId: "A"
}
```




## Variants
 - `N=0, T>=1` node just started, has not seen any request with `Term>0` yet
 - `N>0, T>N` unlikely situation combined with empty log, but possible if no append entries have been received yet
 - `N>0, T==N` possible after an earlier denied vote that incremented the node term but still left the log empty
