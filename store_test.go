package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBoltStoreMessageOrderingRegression(t *testing.T) {
	dir, err := ioutil.TempDir("", "bolt_store_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := newBoltStore(&boltStoreOptions{
		retention:  time.Hour,
		gcInterval: time.Hour,
		path:       filepath.Join(dir, "messages.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	go store.start()
	defer store.close()

	for i := 1; i < 100; i++ {
		store.append("testtopic", nil)
	}

	msgs, err := store.get("testtopic", "", 0)
	if err != nil {
		t.Fatal(err)
	}

	for i, msg := range msgs.Messages {
		expectedIndex := i + 1
		if int(msg.Index) != expectedIndex {
			t.Fatalf("Unexpected message index; want %d, got %d", expectedIndex, msg.Index)
		}
	}
}
