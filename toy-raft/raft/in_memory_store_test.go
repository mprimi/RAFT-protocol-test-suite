package raft

import (
	"testing"
)

func TestInMemoryStorageBasicVotingAndTermChanges(t *testing.T) {
	store := NewInMemoryStorage()
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


	newTerm = uint64(10)
	store.SetTerm(newTerm)
	assertEqual(t, store.GetCurrentTerm(), newTerm)
	assertEqual(t, store.Voted(), false)
	assertEqual(t, store.GetVotedFor(), "")
}

//func TestInMemoryStorage(t *testing.T) {

//db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
//if err != nil {
//panic(fmt.Sprintf("failed to start in-memory datastore: %s", err))
//}

//initialLog := make(logEntries, 0)
//if err := db.Update(func(txn *badger.Txn) error {
//return txn.Set([]byte(LogKey), initialLog.Bytes())
//}); err != nil {
//panic(fmt.Sprintf("failed to initialize log: %s", err))
//}

//store := &InMemoryStorage{
//currentTerm: 0,
//votedFor:    "",
//lastLogIdx:  0,
//offset:      1,

//db: db,
//}

//if err := store.AppendEntry(Entry{Term: 1, Cmd: []byte("foo")}); err != nil {
//t.Fatalf("AppendEntry failed: %v", err)
//}

//if err := db.View(func(txn *badger.Txn) error {
//item, err := txn.Get([]byte(LogKey))
//if err != nil {
//return err
//}
//logInBytes, err := item.ValueCopy(nil)
//if err != nil {
//return err
//}
//entries := LoadLogEntries(logInBytes)
//if len(entries) != 1 {
//t.Fatalf("expected 1 entry, got %d", len(entries))
//}
//if entries[0].LogIndex != 1 {
//t.Fatalf("expected log index 1, got %d", entries[0].LogIndex)
//}
//if string(entries[0].Data) != "foo" {
//t.Fatalf("expected data 'foo', got %s", entries[0].Data)
//}
//return nil
//}); err != nil {
//t.Fatalf("failed to read from db: %v", err)
//}
//}
