package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

func serve(addr string, store messageStore, pushInterval time.Duration) error {
	r := mux.NewRouter()
	r.HandleFunc("/topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var data map[string]interface{}
		if err = json.Unmarshal(body, &data); err != nil {
			http.Error(w, fmt.Sprintf("body is not a valid JSON object: %v", err), http.StatusBadRequest)
			return
		}

		vars := mux.Vars(r)
		if err = store.append(vars["topic"], data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, fmt.Sprintf("invalid method %s", r.Method), http.StatusBadRequest)
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

		vars := mux.Vars(r)
		msgs, err := store.get(vars["topic"], genID, idx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		marshalled, err := json.Marshal(msgs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if _, err := w.Write(marshalled); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}).Methods("GET")

	watchManager := newWatchManager(store, pushInterval)
	r.HandleFunc("/topics/{topic}/watch", watchManager.handleWatchRequest)

	return http.ListenAndServe(addr, r)
}
