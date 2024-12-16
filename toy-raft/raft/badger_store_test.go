package raft

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSetTermAndVote(t *testing.T) {
	storageTypes := map[string]Storage{
		"memory": NewInMemoryStorage(),
		"disk":   NewDiskStorage("test", t.TempDir()),
	}

	for storageType, store := range storageTypes {
		t.Run(storageType, func(t *testing.T) {
			assertEqual(t, store.GetCurrentTerm(), 0)
			assertEqual(t, store.Voted(), false)
			assertEqual(t, store.GetVotedFor(), "")

			newTerm := uint64(2)
			store.SetTerm(newTerm)
			assertEqual(t, store.GetCurrentTerm(), newTerm)
			assertEqual(t, store.Voted(), false)
			assertEqual(t, store.GetVotedFor(), "")

			newVoteId := "CANDIDATE"
			store.VoteFor(newVoteId, newTerm)
			assertEqual(t, store.GetCurrentTerm(), newTerm)
			assertEqual(t, store.Voted(), true)
			assertEqual(t, store.GetVotedFor(), newVoteId)

			incrementedTerm := store.IncrementTerm()
			assertEqual(t, store.GetCurrentTerm(), newTerm+1)
			assertEqual(t, store.GetCurrentTerm(), incrementedTerm)
			assertEqual(t, store.Voted(), false)
			assertEqual(t, store.GetVotedFor(), "")

			newTerm = uint64(10)
			store.SetTerm(newTerm)
			assertEqual(t, store.GetCurrentTerm(), newTerm)
			assertEqual(t, store.Voted(), false)
			assertEqual(t, store.GetVotedFor(), "")
		})
	}
}

func TestLog(t *testing.T) {

	/*
		check if init log is empty
		-> lastLogIdx = 0

		call AE n times and check if all keys exists
		-> lastLogIdx = n
		call TestGetLogEntries

		call AE N times and (partial) delete M entries
		-> lastLogIdx = N-M

		full delete
		-> check empty
	*/

	entriesToAdd := []Entry{
		{
			Term: 1,
			Cmd:  []byte("entry-1"),
		},
		{
			Term: 1,
			Cmd:  []byte("entry-2"),
		},
		{
			Term: 2,
			Cmd:  []byte("entry-3"),
		},
		{
			Term: 2,
			Cmd:  []byte("entry-4"),
		},
		{
			Term: 2,
			Cmd:  []byte("entry-5"),
		},
	}

	storageTypes := map[string]Storage{
		"memory": NewInMemoryStorage(),
		"disk":   NewDiskStorage("test", t.TempDir()),
	}

	for storageType, store := range storageTypes {
		t.Run(storageType, func(t *testing.T) {

			// we cannot append entries with term 0
			store.SetTerm(2)

			assertEqual(t, store.GetLastLogIndex(), 0)

			for _, entry := range entriesToAdd {
				err := store.AppendEntry(entry)
				if err != nil {
					t.Fatal(err)
				}
			}

			assertEqual(t, store.GetLastLogIndex(), uint64(len(entriesToAdd)))

			checkEntries := func(expectedEntries []Entry) {
				// check if all added entries are stored in proper order
				for idx, entry := range expectedEntries {
					logIdx := uint64(idx + 1)
					storedEntry, exists := store.GetLogEntry(logIdx)
					if !exists {
						t.Fatalf("expected log entry to exist at index %d", logIdx)
					}

					if !reflect.DeepEqual(storedEntry, entry) {
						t.Fatalf("index %d, expected %+v actual %+v", logIdx, entry, storedEntry)
					}
				}

				// same check, different retrieval method
				for i := 0; i < len(expectedEntries); i++ {
					startingLogIndex := uint64(i + 1)
					storedEntries := store.GetLogEntriesFrom(startingLogIndex)
					for j, storedEntry := range storedEntries {
						expectedEntry := expectedEntries[i+j]
						if !reflect.DeepEqual(storedEntry, expectedEntry) {
							t.Fatalf("log index %d, expected %+v actual %+v", startingLogIndex+uint64(j), expectedEntry, storedEntry)
						}
					}
				}
			}

			checkEntries(entriesToAdd)

			lastLogIdx, lastLogTerm := store.GetLastLogIndexAndTerm()
			assertEqual(t, lastLogIdx, uint64(len(entriesToAdd)))
			assertEqual(t, lastLogTerm, entriesToAdd[len(entriesToAdd)-1].Term)

			// discard some entries
			store.DeleteEntriesFrom(3)

			retainedEntries := entriesToAdd[:2]
			checkEntries(retainedEntries)
			lastLogIdx, lastLogTerm = store.GetLastLogIndexAndTerm()
			assertEqual(t, lastLogIdx, uint64(len(retainedEntries)))
			assertEqual(t, lastLogTerm, entriesToAdd[len(retainedEntries)-1].Term)

			// discard all entries
			store.DeleteEntriesFrom(1)

			lastLogIdx, lastLogTerm = store.GetLastLogIndexAndTerm()
			assertEqual(t, lastLogIdx, 0)
			assertEqual(t, lastLogTerm, 0)
		})
	}
}

func TestLogPersistence(t *testing.T) {

	entriesToAdd := []*Entry{
		{
			Term: 1,
			Cmd:  []byte("entry-1"),
		},
		{
			Term: 1,
			Cmd:  []byte("entry-2"),
		},
		{
			Term: 2,
			Cmd:  []byte("entry-3"),
		},
		{
			Term: 2,
			Cmd:  []byte("entry-4"),
		},
		{
			Term: 2,
			Cmd:  []byte("entry-5"),
		},
	}

	storeDir := t.TempDir()
	fmt.Printf("storeDir: %v\n", storeDir)
	replicaId := "test"

	store := NewDiskStorage(replicaId, storeDir)
	term := uint64(2)
	vote := "another guy"
	store.SetTerm(term)
	store.VoteFor(vote, term)

	for _, entry := range entriesToAdd {
		if err := store.AppendEntry(*entry); err != nil {
			t.Fatal(err)
		}
	}

	// close
	if err := store.Close(); err != nil {
		return
	}

	// reopen
	store = NewDiskStorage(replicaId, storeDir)

	// check if entries are still there
	storedEntries, logOffset := store.TestGetLogEntries()
	assertEqual(t, logOffset, 0)
	if !reflect.DeepEqual(entriesToAdd, storedEntries) {
		t.Fatalf("expected %+v, actual %+v", entriesToAdd, storedEntries)
	}

	// check if voted for and term are the same
	assertEqual(t, store.GetCurrentTerm(), term)
	assertEqual(t, store.Voted(), true)
	assertEqual(t, store.GetVotedFor(), vote)
}

func TestDeleteEntriesUpTo(t *testing.T) {

	compareLog := func(t *testing.T, storage BadgerStorage, expectedLogEntries []Entry, expectedLogOffset uint64) {
		assertEqual(t, storage.GetFirstLogIndex(), expectedLogOffset+1)
		assertEqual(t, storage.GetLastLogIndex(), uint64(len(expectedLogEntries))+expectedLogOffset)

		// check entries for consistency
		logEntries, logOffset := storage.TestGetLogEntries()
		assertEqual(t, logOffset, expectedLogOffset)
		assertEqual(t, len(logEntries), len(expectedLogEntries))

		for idx, logEntry := range logEntries {
			assertDeepEqual(t, *logEntry, expectedLogEntries[idx])
		}
	}

	testCases := []struct {
		description      string
		initialLog       []Entry
		initialLogOffset uint64
		deleteUpTo       uint64
		expectedLog      []Entry
	}{
		{
			description: "trim log prefix",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("a"),
				},
				{
					Term: 1,
					Cmd:  []byte("b"),
				},
				{
					Term: 2,
					Cmd:  []byte("c"),
				},
			},
			initialLogOffset: 0,
			deleteUpTo:       2,
			expectedLog: []Entry{
				{
					Term: 2,
					Cmd:  []byte("c"),
				},
			},
		},
		{
			description: "trim entire log",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("a"),
				},
				{
					Term: 1,
					Cmd:  []byte("b"),
				},
				{
					Term: 2,
					Cmd:  []byte("c"),
				},
			},
			initialLogOffset: 0,
			deleteUpTo:       3,
			expectedLog:      []Entry{},
		},
		{
			description: "trim previously trimmed log",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("d"),
				},
				{
					Term: 1,
					Cmd:  []byte("e"),
				},
				{
					Term: 2,
					Cmd:  []byte("f"),
				},
			},
			initialLogOffset: 3,
			deleteUpTo:       4,
			expectedLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("e"),
				},
				{
					Term: 2,
					Cmd:  []byte("f"),
				},
			},
		},
		{
			description: "delete previously trimmed log",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("d"),
				},
				{
					Term: 1,
					Cmd:  []byte("e"),
				},
				{
					Term: 2,
					Cmd:  []byte("f"),
				},
			},
			initialLogOffset: 3,
			deleteUpTo:       6,
			expectedLog:      []Entry{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			storage := NewDiskStorage("test", t.TempDir())
			storage.SetTerm(1)

			if tc.initialLogOffset > 0 {
				storage.setFirstLogIdx(tc.initialLogOffset + 1)
			}
			storage.setLastLogIdx(tc.initialLogOffset)

			for _, e := range tc.initialLog {
				if err := storage.AppendEntry(e); err != nil {
					t.Fatal(err)
				}
			}
			// check if initialization is valid
			compareLog(t, *storage, tc.initialLog, tc.initialLogOffset)

			storage.DeleteEntriesUpTo(tc.deleteUpTo)

			compareLog(t, *storage, tc.expectedLog, tc.deleteUpTo)

		})
	}
}

func TestDeleteEntriesUpToPanics(t *testing.T) {
	testCases := []struct {
		description      string
		initialLog       []Entry
		initialLogOffset uint64
		deleteUpTo       uint64
	}{
		{
			description: "invalid trim index of 0",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("d"),
				},
				{
					Term: 1,
					Cmd:  []byte("e"),
				},
				{
					Term: 2,
					Cmd:  []byte("f"),
				},
			},
			initialLogOffset: 3,
			deleteUpTo:       0,
		},
		{
			description: "invalid trim index, below first log index",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("d"),
				},
				{
					Term: 1,
					Cmd:  []byte("e"),
				},
				{
					Term: 2,
					Cmd:  []byte("f"),
				},
			},
			initialLogOffset: 3,
			deleteUpTo:       3,
		},
		{
			description: "invalid trim index, above last log index",
			initialLog: []Entry{
				{
					Term: 1,
					Cmd:  []byte("d"),
				},
				{
					Term: 1,
					Cmd:  []byte("e"),
				},
				{
					Term: 2,
					Cmd:  []byte("f"),
				},
			},
			initialLogOffset: 3,
			deleteUpTo:       7,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			storage := NewDiskStorage("test", t.TempDir())
			storage.SetTerm(1)

			storage.setFirstLogIdx(tc.initialLogOffset + 1)
			storage.setLastLogIdx(tc.initialLogOffset)

			for _, e := range tc.initialLog {
				if err := storage.AppendEntry(e); err != nil {
					t.Fatal(err)
				}
			}

			defer func() {
				if err := recover(); err == nil {
					t.Fatal("expected panic")
				}
			}()

			storage.DeleteEntriesUpTo(tc.deleteUpTo)
		})
	}
}

func TestPrependEntry(t *testing.T) {
	compareLog := func(t *testing.T, storage *BadgerStorage, expectedLogEntries []Entry, expectedFirstLogIndex uint64, expectedLastLogIndex uint64) {
		t.Helper()
		assertEqual(t, storage.GetFirstLogIndex(), expectedFirstLogIndex)
		assertEqual(t, storage.GetLastLogIndex(), expectedLastLogIndex)

		// check entries for consistency
		for i := expectedFirstLogIndex; i <= expectedLastLogIndex; i++ {
			entry, exists := storage.GetLogEntry(i)
			if !exists {
				t.Fatalf("failed to get entry %d", i)
			}
			assertDeepEqual(t, entry, expectedLogEntries[i-expectedFirstLogIndex])
		}
	}

	storage := NewDiskStorage("test", t.TempDir())
	storage.SetTerm(1)

	initialLog := []Entry{
		{
			Term: 1,
			Cmd:  []byte("a"),
		},
		{
			Term: 1,
			Cmd:  []byte("b"),
		},
		{
			Term: 2,
			Cmd:  []byte("c"),
		},
		{
			Term: 2,
			Cmd:  []byte("d"),
		},
		{
			Term: 2,
			Cmd:  []byte("e"),
		},
	}

	// populate with 5 entries
	for _, entry := range initialLog {
		if err := storage.AppendEntry(entry); err != nil {
			t.Fatal(err)
		}
	}

	// trim 2
	storage.DeleteEntriesUpTo(2)
	compareLog(t, storage, initialLog[2:], 3, 5)

	// prepend them both back
	storage.PrependEntry(initialLog[1])
	storage.PrependEntry(initialLog[0])

	// check that its equal to initial log
	compareLog(t, storage, initialLog, 1, 5)
}
