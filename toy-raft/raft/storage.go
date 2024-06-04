package raft

type Storage interface {
	Commit() error

	GetCurrentTerm() uint64
	// zeroes votedFor as a side effect
	IncrementTerm() uint64
	// zeroes votedFor as a side effect
	SetTerm(uint64)
	GetVotedFor() string
	Voted() bool
	VoteFor(id string, currentTerm uint64)
	// Log Methods
	AppendEntry(entry Entry) error
	GetLastLogIndex() uint64
	GetLastLogIndexAndTerm() (index uint64, term uint64)
	DeleteEntriesFrom(index uint64)
	GetLogEntriesFrom(index uint64) []Entry
	TestGetLogEntries() []*Entry
	GetLogEntry(index uint64) (*Entry, bool)
}
