package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

type watchManager struct {
	store        messageStore
	pushInterval time.Duration
}

func newWatchManager(store messageStore, pushInterval time.Duration) *watchManager {
	return &watchManager{
		store:        store,
		pushInterval: pushInterval,
	}
}

type activeWatch struct {
	wm      *watchManager
	topic   string
	genID   string
	idx     uint64
	cw      io.WriteCloser
	flusher http.Flusher
}

func newActiveWatch(wm *watchManager, topic string, genID string, idx uint64, cw io.WriteCloser, flusher http.Flusher) *activeWatch {
	return &activeWatch{
		wm:      wm,
		topic:   topic,
		genID:   genID,
		cw:      cw,
		idx:     idx,
		flusher: flusher,
	}
}

func (wm *watchManager) handleWatchRequest(w http.ResponseWriter, r *http.Request) {
	topic, ok := mux.Vars(r)["topic"]
	if !ok {
		log.Printf("Error: topic not provided")
		http.Error(w, "must provide topic", http.StatusBadRequest)
		return
	}

	genID := r.URL.Query().Get("generationID")
	fromIdx := r.URL.Query().Get("fromIndex")

	if fromIdx == "" {
		fromIdx = "0"
	}

	idx, err := strconv.ParseUint(fromIdx, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid 'fromIndex': %v", err), http.StatusBadRequest)
		return
	}
	log.Printf("Connection accepted from %v", r.RemoteAddr)
	if err = wm.manageWatch(w, topic, genID, idx); err != nil {
		log.Printf("Error: watch %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (wm *watchManager) manageWatch(w http.ResponseWriter, topic string, genID string, idx uint64) error {
	cn, canNotifyClose := w.(http.CloseNotifier)
	flusher, canFlush := w.(http.Flusher)
	if !canNotifyClose || !canFlush {
		return errors.New("Error: cannot stream")
	}
	aw := newActiveWatch(wm, topic, genID, idx, httputil.NewChunkedWriter(w), flusher)
	defer aw.close()
	for {
		select {
		case <-cn.CloseNotify():
			return nil
		default:
			if err := aw.handleNewMessages(); err != nil {
				return err
			}
		}
		time.Sleep(wm.pushInterval)
	}
}

func (aw *activeWatch) handleNewMessages() error {
	var err error
	var msgsResponse *MessagesResponse
	if msgsResponse, err = aw.newMessages(); err != nil {
		return err
	}
	if msgsLength := len(msgsResponse.Messages); msgsLength > 0 {
		if err := aw.writeChunk(msgsResponse); err != nil {
			return err
		}
		aw.genID = msgsResponse.GenerationID
		aw.idx = msgsResponse.Messages[msgsLength-1].Index + 1
	}
	return nil
}

func (aw *activeWatch) newMessages() (*MessagesResponse, error) {
	return aw.wm.store.get(aw.topic, aw.genID, aw.idx)
}

func (aw *activeWatch) writeChunk(msgs *MessagesResponse) error {
	marshalled, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	if _, err = aw.cw.Write(marshalled); err != nil {
		return err
	}
	aw.flusher.Flush()
	return nil
}

func (aw *activeWatch) close() {
	log.Println("Connection closed by peer")
	if err := aw.cw.Close(); err != nil {
		log.Printf("Error: closing connection %v\n", err)
	}
}
