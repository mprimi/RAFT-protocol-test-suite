package raft

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var (
	TermKey       = []byte("term")
	VoteKey       = []byte("vote")
	LastLogIdxKey = []byte("lastLogIdx")
)

type InMemoryStorage struct {
	db *badger.DB
}

type logEntry struct {
	Data []byte
	Term uint64
}

func NewInMemoryStorage() Storage {

	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(fmt.Sprintf("failed to start in-memory datastore: %s", err))
	}

	store := &InMemoryStorage{
		db: db,
	}

	// initialize term
	store.SetTerm(0)

	// initialize log
	store.setLastLogIdx(0)

	return store
}

func (store *InMemoryStorage) setLastLogIdx(newLastLogIdx uint64) {

	currentLastLogIdx := store.GetLastLogIndex()
	if currentLastLogIdx == 0 && newLastLogIdx == 0 {
		// initial case, don't panic
	} else if newLastLogIdx == currentLastLogIdx {
		panic(fmt.Sprintf("attempted to set lastLogIdx to %d when it was %d", newLastLogIdx, currentLastLogIdx))
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, newLastLogIdx)

	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(LastLogIdxKey, buf); err != nil {
			return fmt.Errorf("failed to set lastLogIdx: %s", err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func (store *InMemoryStorage) GetLogEntry(idx uint64) (*Entry, bool) {
	if idx == 0 {
		panic("index cannot be zero")
	}

	lastLogIdx := store.GetLastLogIndex()
	if idx > lastLogIdx {
		return nil, false
	}

	var entry *Entry
	err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(store.idxToKey(idx))
		if err != nil {
			return fmt.Errorf("failed to get store entry at index %d: %w", idx, err)
		}

		if err := item.Value(func(val []byte) error {
			x := LoadEntry(val)
			entry = &x
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return entry, true
}

func (store *InMemoryStorage) TestGetLogEntries() []*Entry {
	lastLogIdx := store.GetLastLogIndex()
	entries := make([]*Entry, 0, lastLogIdx)
	err := store.db.View(func(txn *badger.Txn) error {
		for idx := uint64(1); idx <= lastLogIdx; idx++ {

			item, err := txn.Get(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to get store entry at index %d: %w", idx, err)
			}

			if err := item.Value(func(val []byte) error {
				x := LoadEntry(val)
				entries = append(entries, &x)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return entries
}

func (store *InMemoryStorage) DeleteEntriesFrom(startingLogIdx uint64) {
	if startingLogIdx == 0 {
		panic("cannot delete from 0, indexes start at 1")
	}
	lastLogIdx := store.GetLastLogIndex()
	err := store.db.Update(func(txn *badger.Txn) error {
		for idx := startingLogIdx; idx <= lastLogIdx; idx++ {
			err := txn.Delete(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to delete key at index %d: %w", idx, err)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	store.setLastLogIdx(startingLogIdx - 1)
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

	var lastLogIdx uint64
	store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(LastLogIdxKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist yet
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to get lastLogIdx: %w", err)
		}

		err = item.Value(func(val []byte) error {
			lastLogIdx = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read value of lastLogIdx item: %w", err)
		}
		return nil
	})
	return lastLogIdx
}

func (store *InMemoryStorage) idxToKey(idx uint64) []byte {
	return []byte(fmt.Sprintf("%d", idx))
}

func (store *InMemoryStorage) AppendEntry(entry Entry) error {

	lastLogIdx := store.GetLastLogIndex()
	entryIdx := lastLogIdx + 1

	key := store.idxToKey(entryIdx)
	value := entry.Bytes()

	// put in log
	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to append entry at index %d: %w", entryIdx, err)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	// update lastLogIdx
	store.setLastLogIdx(entryIdx)

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

func (store *InMemoryStorage) GetLogEntriesFrom(startingLogIdx uint64) []Entry {
	lastLogIdx := store.GetLastLogIndex()
	entries := make([]Entry, 0, lastLogIdx-startingLogIdx+1)
	err := store.db.View(func(txn *badger.Txn) error {
		for idx := startingLogIdx; idx <= lastLogIdx; idx++ {

			item, err := txn.Get(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to get store entry at index %d: %w", idx, err)
			}

			if err := item.Value(func(val []byte) error {
				entries = append(entries, LoadEntry(val))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return entries
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
