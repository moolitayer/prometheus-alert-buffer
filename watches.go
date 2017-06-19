package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

type watchManager struct {
	upgrader     *websocket.Upgrader
	store        messageStore
	pushInterval time.Duration
}

func newWatchManager(store messageStore, pushInterval time.Duration) *watchManager {
	return &watchManager{
		upgrader:     &websocket.Upgrader{},
		store:        store,
		pushInterval: pushInterval,
	}
}

func (wm *watchManager) handleWatchRequest(w http.ResponseWriter, r *http.Request) {
	topic, ok := mux.Vars(r)["topic"]
	if !ok {
		log.Printf("Error: topic not provided")
		http.Error(w, "must provide topic", http.StatusBadRequest)
		return
	}

	conn, err := wm.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade HTTP connection: %v", err)
		http.Error(w, fmt.Sprintf("failed to upgrade HTTP connection: %v", err), http.StatusInternalServerError)
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

	go wm.manageWatch(conn, topic, genID, idx)
}

func (wm *watchManager) manageWatch(conn *websocket.Conn, topic, genID string, idx uint64) {
	log.Printf("Connection accepted from %v", conn.RemoteAddr())
	defer closeConn(conn)
	for {
		msgsResponse, err := wm.store.get(topic, genID, idx)
		if err != nil {
			handleError(err, conn)
			return
		}
		if msgsLength := len(msgsResponse.Messages); msgsLength > 0 {
			if err := conn.WriteJSON(msgsResponse); err != nil {
				handleError(err, conn)
				return
			}
			idx = msgsResponse.Messages[msgsLength-1].Index+1
		}
		time.Sleep(wm.pushInterval)
	}
}

func closeConn(conn *websocket.Conn) {
	log.Printf("Terminating connection to %v", conn.RemoteAddr())
	if err := conn.Close(); err != nil {
		log.Printf("[WARNING] error closing connection: %v", err)
	}
}

func handleError(err error, conn *websocket.Conn) error {
	log.Printf("Closing connection due to error: %v", err)
	return conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error()))
}
