package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func newTestBoltStore(t *testing.T) (store *boltStore, close func()) {
	dir, err := ioutil.TempDir("", "bolt_store_test_")
	if err != nil {
		t.Fatal(err)
	}

	store, err = newBoltStore(&boltStoreOptions{
		retention:  time.Hour,
		gcInterval: time.Hour,
		path:       filepath.Join(dir, "messages.db"),
		registry:   prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	go store.start()

	return store, func() {
		store.close()
		os.RemoveAll(dir)
	}
}

func TestBoltStoreMessageOrderingRegression(t *testing.T) {
	store, close := newTestBoltStore(t)
	defer close()

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

func TestStoreMetrics(t *testing.T) {
	store, close := newTestBoltStore(t)
	defer close()

	for i := 0; i < 5; i++ {
		if err := store.append("topicA", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := store.append("topicB", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 15; i++ {
		if _, err := store.get("topicA", "", 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := store.get("topicB", "", 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		if _, err := store.gc(time.Now().Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	rw := httptest.NewRecorder()
	h := promhttp.HandlerFor(store.options.registry, promhttp.HandlerOpts{})
	h.ServeHTTP(rw, &http.Request{})

	wantMetrics, err := ioutil.ReadFile("fixtures/store_metrics.txt")
	if err != nil {
		t.Fatalf("Unable to read input test file: %v", err)
	}

	wantLines := strings.Split(string(wantMetrics), "\n")
	gotLines := strings.Split(string(rw.Body.String()), "\n")

	ignoreRe := regexp.MustCompile(`^message_store_gc_duration_seconds_sum `)

	// Until the Prometheus Go client library offers better testability
	// (https://github.com/prometheus/client_golang/issues/58), we simply compare
	// verbatim text-format metrics outputs, but ignore certain metric lines
	// whose value is hard to control.
	for i, want := range wantLines {
		if ignoreRe.MatchString(want) {
			continue
		}
		if want != gotLines[i] {
			t.Fatalf("unexpected metric line\nwant: %s\nhave: %s", want, gotLines[i])
		}
	}
}
