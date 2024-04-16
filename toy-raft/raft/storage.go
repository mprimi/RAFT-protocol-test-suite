package raft

import "fmt"

/*
When mutating term, votedFor gets cleared
*/
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
	AppendEntry(entry *Entry)
	GetLastLogIndex() uint64
	GetLastLogIndexAndTerm() (index uint64, term uint64)
	DeleteEntriesFrom(index uint64)
	GetLogEntriesFrom(index uint64) []Entry
	TestGetLogEntries() []*Entry
	GetLogEntry(index uint64) (*Entry, bool)
}

type InMemoryStorage struct {
	currentTerm uint64
	votedFor    string
	log         []*Entry
	lastLogIdx  uint64
	offset      uint64
}

func NewInMemoryStorage() Storage {
	return &InMemoryStorage{
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*Entry, 0),
		lastLogIdx:  0,
		offset:      1,
	}
}

func (store *InMemoryStorage) Commit() error {
	return nil
}

func (store *InMemoryStorage) toLogIndex(arrIdx int) uint64 {
	return uint64(arrIdx) + store.offset
}

func (store *InMemoryStorage) toArrIndex(logIdx uint64) int {
	if logIdx < store.offset {
		panic(fmt.Sprintf("log index %d is less than offset %d", logIdx, store.offset))
	}
	return int(logIdx - store.offset)
}

func (store *InMemoryStorage) GetLogEntry(logIdx uint64) (*Entry, bool) {
	if logIdx == 0 {
		panic("log is 1-based, should not ask for index 0")
	}
	if logIdx > store.lastLogIdx {
		return nil, false
	}
	arrIdx := store.toArrIndex(logIdx)
	if arrIdx >= len(store.log) {
		panic(fmt.Sprintf("array index %d is greater than log length %d", arrIdx, len(store.log)))
	}
	return store.log[arrIdx], true
}

func (store *InMemoryStorage) TestGetLogEntries() []*Entry {
	return store.log
}

func (store *InMemoryStorage) DeleteEntriesFrom(logIdx uint64) {
	if logIdx > store.lastLogIdx {
		panic(fmt.Sprintf("log index %d is greater than last log index %d", logIdx, store.lastLogIdx))
	}
	arrIdx := store.toArrIndex(logIdx)
	if arrIdx >= len(store.log) {
		panic(fmt.Sprintf("log index %d is greater than last log index %d", logIdx, store.lastLogIdx))
	}
	store.log = store.log[:arrIdx]
	store.lastLogIdx = logIdx - 1
	if uint64(len(store.log))+store.offset-1 != store.lastLogIdx {
		panic(fmt.Sprintf("log length %d + offset %d != last log index %d", len(store.log), store.offset, store.lastLogIdx))
	}
}

func (store *InMemoryStorage) GetLastLogIndexAndTerm() (index uint64, term uint64) {
	index = store.GetLastLogIndex()
	if index == 0 {
		term = 0
		return
	}
	entry, exists := store.GetLogEntry(index)
	if !exists {
		panic(fmt.Sprintf("no entry found at last log index: %d", index))
	}
	term = entry.Term
	return
}

func (store *InMemoryStorage) GetLastLogIndex() uint64 {
	return store.lastLogIdx
}

func (store *InMemoryStorage) AppendEntry(entry *Entry) {
	store.log = append(store.log, entry)
	store.lastLogIdx++
	if uint64(len(store.log))+store.offset-1 != store.lastLogIdx {
		panic(fmt.Sprintf("log length %d + offset %d != last log index %d", len(store.log), store.offset, store.lastLogIdx))
	}
}

func (store *InMemoryStorage) GetCurrentTerm() uint64 {
	return store.currentTerm
}

func (store *InMemoryStorage) GetVotedFor() string {
	return store.votedFor
}

func (store *InMemoryStorage) Voted() bool {
	return store.votedFor != ""
}

func (store *InMemoryStorage) SetTerm(term uint64) {
	if store.GetCurrentTerm() >= term {
		panic(fmt.Sprintf("attempted to set term to %d when it was %d", term, store.GetCurrentTerm()))
	}
	store.currentTerm = term
	// clear voted for
	store.votedFor = ""
}

func (store *InMemoryStorage) IncrementTerm() uint64 {
	store.SetTerm(store.currentTerm + 1)
	return store.currentTerm
}

func (store *InMemoryStorage) VoteFor(id string, currentTerm uint64) {
	if store.currentTerm != currentTerm {
		panic(fmt.Sprintf("tried to vote for term %d, but current term is %d", currentTerm, store.currentTerm))
	}
	if store.votedFor != "" {
		panic(fmt.Sprintf("tried to vote for %s but already voted for %s", id, store.votedFor))
	}
	store.votedFor = id
}

func (store *InMemoryStorage) GetLogEntriesFrom(logIdx uint64) []Entry {
	if logIdx > store.lastLogIdx {
		return make([]Entry, 0)
	}
	arrIdx := store.toArrIndex(logIdx)
	entries := store.log[arrIdx:]
	res := make([]Entry, len(entries))
	for i, entry := range entries {
		res[i] = *entry
	}
	return res
}
