package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/boltdb/bolt"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
)

const (
	bucketMetadata = "metadata"
	bucketMessages = "messages"

	keyGenerationID = "generationID"
)

type messageStore interface {
	append(topic string, data interface{}) error
	get(topic string, generationID string, fromIndex uint64) (*MessagesResponse, error)
}

type boltStore struct {
	db           *bolt.DB
	generationID string
	options      *boltStoreOptions

	totalAppends  *prometheus.CounterVec
	failedAppends *prometheus.CounterVec
	totalGets     *prometheus.CounterVec
	failedGets    *prometheus.CounterVec
	gcDuration    prometheus.Histogram

	stop chan struct{}
	done chan struct{}
}

type boltStoreOptions struct {
	retention  time.Duration
	gcInterval time.Duration
	path       string

	registry *prometheus.Registry
}

func newBoltStore(opts *boltStoreOptions) (*boltStore, error) {
	db, err := bolt.Open(opts.path, 0600, nil)
	if err != nil {
		return nil, err
	}

	store := &boltStore{
		db:      db,
		options: opts,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),

		totalAppends: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "message_store_appends_total",
			Help: "The total number of messages appended (including append failures) to the message store by topic.",
		}, []string{"topic"}),
		failedAppends: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "message_store_appends_failed_total",
			Help: "The total number of failed appends to the message store by topic.",
		}, []string{"topic"}),
		totalGets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "message_store_gets_total",
			Help: "The total number of retrieved messages (including retrieval failures) from the message store by topic.",
		}, []string{"topic"}),
		failedGets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "message_store_gets_failed_total",
			Help: "The total number of failed retrievals from the message store by topic.",
		}, []string{"topic"}),
		gcDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "message_store_gc_duration_seconds",
			Help:    "The distribution of message store garbage collection cycle durations in seconds.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
		}),
	}

	if opts.registry != nil {
		opts.registry.Register(store.totalAppends)
		opts.registry.Register(store.failedAppends)
		opts.registry.Register(store.totalGets)
		opts.registry.Register(store.failedGets)
		opts.registry.Register(store.gcDuration)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketMessages))
		if err != nil {
			return fmt.Errorf("error creating messages bucket: %v", err)
		}

		b, err := tx.CreateBucketIfNotExists([]byte(bucketMetadata))
		if err != nil {
			return fmt.Errorf("error creating metadata bucket: %v", err)
		}
		genID := b.Get([]byte(keyGenerationID))
		if genID == nil {
			genID = []byte(uuid.NewV4().String())
			if err := b.Put([]byte(keyGenerationID), genID); err != nil {
				return fmt.Errorf("error initializing generation ID: %v", err)
			}
		}
		store.generationID = string(genID)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return store, nil
}

func (bs *boltStore) start() {
	gcTicker := time.NewTicker(bs.options.gcInterval)
	for {
		select {
		case <-bs.stop:
			close(bs.done)
			return
		case <-gcTicker.C:
			log.Println("Running GC cycle to remove old entries...")
			num, err := bs.gc(time.Now().Add(-bs.options.retention))
			if err != nil {
				log.Println("Error running GC cycle:", err)
			} else {
				log.Printf("Deleted %d old entries", num)
			}
		}
	}
}

func keyFromIndex(index uint64) []byte {
	buf := make([]byte, 8)
	// This needs to be BigEndian so that keys are stored in numeric sort order.
	binary.BigEndian.PutUint64(buf, index)
	return buf
}

func (bs *boltStore) append(topic string, data interface{}) error {
	err := bs.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketMessages))
		b, err := root.CreateBucketIfNotExists([]byte(topic))
		if err != nil {
			return fmt.Errorf("error creating bucket for topic %q: %v", topic, err)
		}
		idx, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("error getting next sequence number: %v", err)
		}

		n := Message{
			Index:     idx,
			Timestamp: time.Now(),
			Data:      data,
		}
		buf, err := json.Marshal(n)
		if err != nil {
			return fmt.Errorf("error marshalling message: %v", err)
		}
		if err := b.Put(keyFromIndex(idx), buf); err != nil {
			return fmt.Errorf("error appending message: %v", err)
		}
		return nil
	})

	bs.totalAppends.WithLabelValues(topic).Inc()
	if err != nil {
		bs.failedAppends.WithLabelValues(topic).Inc()
	}
	return err
}

func (bs *boltStore) get(topic string, generationID string, fromIndex uint64) (*MessagesResponse, error) {
	ns := []Message{}
	err := bs.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketMessages))
		b := root.Bucket([]byte(topic))
		if b == nil {
			// Topic doesn't exist yet, return it as an empty set.
			return nil
		}
		c := b.Cursor()

		var k, v []byte
		if generationID == bs.generationID {
			k, v = c.Seek(keyFromIndex(fromIndex))
		} else {
			k, v = c.First()
		}

		var n Message
		for ; k != nil; k, v = c.Next() {
			if err := json.Unmarshal(v, &n); err != nil {
				return fmt.Errorf("unable to unmarshal message: %v", err)
			}

			ns = append(ns, n)
		}
		return nil
	})

	bs.totalGets.WithLabelValues(topic).Inc()

	if err != nil {
		bs.failedGets.WithLabelValues(topic).Inc()
		return nil, err
	}

	return &MessagesResponse{
		GenerationID: bs.generationID,
		Messages:     ns,
	}, nil
}

func (bs *boltStore) gc(olderThan time.Time) (int, error) {
	start := time.Now()
	defer func() {
		bs.gcDuration.Observe(float64(time.Since(start).Seconds()))
	}()

	var numDeleted int
	return numDeleted, bs.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketMessages))
		rootC := root.Cursor()

		for topic, _ := rootC.First(); topic != nil; topic, _ = rootC.Next() {
			c := root.Bucket(topic).Cursor()

			// For now, this goes through all entries and doesn't abort after the first
			// encountered entry that should be kept, just in case there are time/date
			// glitches on a machine and timestamps end up being out of order.
			//
			// TODO: Possibly reconsider this for performance reasons if the DB gets huge.
			var n Message
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if err := json.Unmarshal(v, &n); err != nil {
					return fmt.Errorf("unable to unmarshal message: %v", err)
				}

				if n.Timestamp.Before(olderThan) {
					if err := c.Delete(); err != nil {
						return fmt.Errorf("unable to delete message: %v", err)
					}
					numDeleted++
				}
			}
		}
		return nil
	})
}

func (bs *boltStore) close() error {
	close(bs.stop)
	<-bs.done
	return bs.db.Close()
}
