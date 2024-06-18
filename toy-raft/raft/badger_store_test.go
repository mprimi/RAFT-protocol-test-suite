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

					if !reflect.DeepEqual(storedEntry, &entry) {
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
	storedEntries := store.TestGetLogEntries()
	if !reflect.DeepEqual(entriesToAdd, storedEntries) {
		t.Fatalf("expected %+v, actual %+v", entriesToAdd, storedEntries)
	}

	// check if voted for and term are the same
	assertEqual(t, store.GetCurrentTerm(), term)
	assertEqual(t, store.Voted(), true)
	assertEqual(t, store.GetVotedFor(), vote)
}
