package raft

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var (
	TermKey = []byte("term")
	VoteKey = []byte("vote")
)

type InMemoryStorage struct {
	lastLogIdx uint64
	offset     uint64

	db *badger.DB
}

type logEntry struct {
	LogIndex uint64 // 1-based
	Data     []byte
	Term     uint64
}

func NewInMemoryStorage() Storage {

	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(fmt.Sprintf("failed to start in-memory datastore: %s", err))
	}

	store := &InMemoryStorage{
		lastLogIdx: 0,
		offset:     1,

		db: db,
	}

	// initialize term
	store.SetTerm(0)

	return store
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
	return nil, false
}

func (store *InMemoryStorage) TestGetLogEntries() []*Entry {
	return nil
}

func (store *InMemoryStorage) DeleteEntriesFrom(logIdx uint64) {
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

func (store *InMemoryStorage) AppendEntry(entry Entry) error {

	//var logLength int
	//if err := store.db.Update(func(txn *badger.Txn) error {

	//item, err := txn.Get([]byte(LogKey))
	//switch err {
	//case badger.ErrKeyNotFound:
	//// no log yet
	//panic("no log yet")
	//case nil:
	//// log exists
	//default:
	//return fmt.Errorf("failed to get log: %s", err)
	//}

	//logBytes := make([]byte, item.ValueSize())
	//if _, err := item.ValueCopy(logBytes); err != nil {
	//return fmt.Errorf("failed to copy log bytes: %s", err)
	//}

	//logEntries := LoadLogEntries(logBytes)

	//newLogEntry := logEntry{
	//LogIndex: store.lastLogIdx + 1,
	//Data:     entry.Cmd,
	//Term:     entry.Term,
	//}
	//logEntries = append(logEntries, newLogEntry)

	//logLength = len(logEntries)

	//return txn.Set([]byte(LogKey), logEntries.Bytes())
	//}); err != nil {
	//return fmt.Errorf("failed to append entry: %s", err)
	//}
	//store.lastLogIdx++
	//if uint64(logLength)+store.offset-1 != store.lastLogIdx {
	//panic(fmt.Sprintf("log length %d + offset %d != last log index %d", logLength, store.offset, store.lastLogIdx))
	//}

	return nil
}

func (store *InMemoryStorage) VoteFor(id string, currentTerm uint64) {
	storedCurrentTerm := store.GetCurrentTerm()
	if storedCurrentTerm != currentTerm {
		panic(fmt.Sprintf("tried to vote for term %d, but current term is %d", currentTerm, storedCurrentTerm))
	}

	storedVote := store.GetVotedFor()
	if storedVote != "" {
		panic(fmt.Sprintf("tried to vote for %s but already voted for %s", id, storedVote))
	}

	if err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(VoteKey, []byte(id))
	}); err != nil {
		panic("failed to update voted for")
	}
}

func (store *InMemoryStorage) GetLogEntriesFrom(logIdx uint64) []Entry {
	return nil
}

func (store *InMemoryStorage) GetVotedFor() string {
	votedFor := ""
	if err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(VoteKey)
		if err != nil {
			return err
		}
		item.Value(func(val []byte) error {
			votedFor = string(val)
			return nil
		})
		return nil
	}); err != nil {
		panic(fmt.Sprintf("failed to read voted: %s", err))
	}

	return votedFor
}

func (store *InMemoryStorage) Voted() bool {
	return store.GetVotedFor() != ""
}

// clears vote
func (store *InMemoryStorage) SetTerm(term uint64) {
	currentTerm := store.GetCurrentTerm()
	if currentTerm == 0 && term == 0 {
		// initial case, don't panic
	} else if term <= currentTerm {
		panic(fmt.Sprintf("attempted to set term to %d when it was %d", term, store.GetCurrentTerm()))
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, term)

	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TermKey, buf); err != nil {
			return fmt.Errorf("failed to set term: %w", err)
		}
		if err := txn.Set(VoteKey, []byte("")); err != nil {
			return fmt.Errorf("failed to clear vote: %w", err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func (store *InMemoryStorage) GetCurrentTerm() uint64 {
	var term uint64
	store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(TermKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist yet
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to get term: %w", err)
		}

		err = item.Value(func(val []byte) error {
			term = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read value of term item: %w", err)
		}
		return nil
	})
	return term
}

func (store *InMemoryStorage) IncrementTerm() uint64 {
	newTerm := store.GetCurrentTerm() + 1
	store.SetTerm(newTerm)
	return newTerm
}
