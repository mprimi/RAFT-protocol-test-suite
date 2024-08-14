package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
)

var (
	TermKey       = []byte("term")
	VoteKey       = []byte("vote")
	LastLogIdxKey = []byte("lastLogIdx")
)

type BadgerStorage struct {
	db *badger.DB
}

type logEntry struct {
	Data []byte
	Term uint64
}

func NewDiskStorage(replicaId string, baseDir string) Storage {
	dbPath := filepath.Join(baseDir, replicaId)
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		panic(fmt.Errorf("failed to initialize database at %s: %w", dbPath, err))
	}

	store := &BadgerStorage{
		db: db,
	}

	// if term is found, this is an already existing db
	var keyExists bool
	err = db.View(func(txn *badger.Txn) error {
		_, err = txn.Get(TermKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist
		} else if err != nil {
			return err
		} else {
			keyExists = true
		}
		return nil
	})
	if err != nil {
		panic(fmt.Errorf("failed to check term during init: %w", err))
	}
	if !keyExists {
		store.storageInit()
	}

	return store
}

func NewInMemoryStorage() Storage {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(fmt.Errorf("failed to init memory store: %w", err))
	}

	store := &BadgerStorage{
		db: db,
	}
	store.storageInit()

	return store
}

func (store *BadgerStorage) Close() error {
	return store.db.Close()
}

func (store *BadgerStorage) storageInit() {
	// initialize term
	store.SetTerm(0)

	// initialize log
	store.setLastLogIdx(0)
}

func (store *BadgerStorage) setLastLogIdx(newLastLogIdx uint64) {

	currentLastLogIdx := store.GetLastLogIndex()
	if currentLastLogIdx == 0 && newLastLogIdx == 0 {
		// initial case, don't panic
	} else if newLastLogIdx == currentLastLogIdx {
		panic(fmt.Errorf("setting invalid last log index"))
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, newLastLogIdx)

	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(LastLogIdxKey, buf); err != nil {
			return fmt.Errorf("failed to set lastLogIdx: %s", err)
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to commit last log index: %w", err))
	}
}

func (store *BadgerStorage) GetLogEntry(idx uint64) (*Entry, bool) {
	if idx == 0 {
		panic(fmt.Errorf("invalid entry lookup index"))
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
		panic(fmt.Errorf("failed to load entry: %w", err))
	}

	return entry, true
}

func (store *BadgerStorage) TestGetLogEntries() []*Entry {
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

func (store *BadgerStorage) DeleteEntriesFrom(startingLogIdx uint64) {
	if startingLogIdx == 0 {
		panic(fmt.Errorf("invalid delete start index"))
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
		panic(fmt.Errorf("failed to delete: %w", err))
	}
	store.setLastLogIdx(startingLogIdx - 1)
}

func (store *BadgerStorage) GetLastLogIndexAndTerm() (index uint64, term uint64) {
	index = store.GetLastLogIndex()
	if index == 0 {
		term = 0
		return
	}
	entry, exists := store.GetLogEntry(index)
	if !exists {
		panic(fmt.Errorf("expected existing entry not found"))
	}
	term = entry.Term
	return
}

func (store *BadgerStorage) GetLastLogIndex() uint64 {

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

func (store *BadgerStorage) idxToKey(idx uint64) []byte {
	return []byte(fmt.Sprintf("%d", idx))
}

func (store *BadgerStorage) AppendEntry(entry Entry) error {
	if store.GetCurrentTerm() == 0 {
		panic("append entry with zero term")
	}

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
		panic(fmt.Errorf("failed to append entry: %w", err))
	}

	// update lastLogIdx
	store.setLastLogIdx(entryIdx)

	return nil
}

func (store *BadgerStorage) VoteFor(id string, currentTerm uint64) {
	storedCurrentTerm := store.GetCurrentTerm()
	if storedCurrentTerm != currentTerm {
		panic(fmt.Errorf("unexpected term during vote commit"))
	}

	storedVote := store.GetVotedFor()
	if storedVote != "" {
		panic(fmt.Errorf("already voted in this term"))
	}

	if err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(VoteKey, []byte(id))
	}); err != nil {
		panic(fmt.Errorf("failed to update vote: %w", err))
	}
}

func (store *BadgerStorage) GetLogEntriesFrom(startingLogIdx uint64) []Entry {
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
		panic(fmt.Errorf("failed to get entries: %w", err))
	}
	return entries
}

func (store *BadgerStorage) GetVotedFor() string {
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
		panic(fmt.Errorf("failed to load vote: %w", err))
	}

	return votedFor
}

func (store *BadgerStorage) Voted() bool {
	return store.GetVotedFor() != ""
}

// clears vote
func (store *BadgerStorage) SetTerm(term uint64) {
	currentTerm := store.GetCurrentTerm()
	if currentTerm == 0 && term == 0 {
		// initial case, don't panic
	} else if term <= currentTerm {
		panic(fmt.Errorf("attempting to decrease term"))
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
		panic(fmt.Errorf("failed to commit new term and clear vote: %w", err))
	}
}

func (store *BadgerStorage) GetCurrentTerm() uint64 {
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

func (store *BadgerStorage) IncrementTerm() uint64 {
	newTerm := store.GetCurrentTerm() + 1
	store.SetTerm(newTerm)
	return newTerm
}
