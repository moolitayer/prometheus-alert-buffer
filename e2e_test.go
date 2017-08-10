package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const (
	listenAddr = ":9099"
)

var (
	serverStarted bool
)

func initServer(dir string, t *testing.T) {
	if serverStarted {
		return
	}
	retention := 24 * time.Hour
	gcInterval := 10 * time.Minute
	pushInterval := 1 * time.Millisecond
	serverStarted = true
	go func() {
		storagePath := filepath.Join(dir, "messages.db")
		t.Logf("starting server")
		err := runService(storagePath, listenAddr, retention, gcInterval, pushInterval)
		t.Fatalf("server encountered unexpected error: %v", err)
	}()
	if err := waitServerStart(); err != nil {
		t.Fatalf("server encountered unexpected error: %v", err)
	}
}

func TestE2EAppendGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "e2e_test_")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)
	initServer(dir, t)
	genID, err := getGenerationID()
	if err != nil {
		t.Fatalf("failed to retrieve generation ID from server: %v", err)
	}

	//since we don't clean up db from other tests, make sure topics are unique
	topics := []string{
		"topic0",
		"topic1",
		"topic2",
		"topic3",
	}
	items := []map[string]interface{}{
		{"A": "Hi", "B": 0.0},
		{"A": "Hello", "B": 1.0},
		{"A": "Bonjour", "B": 2.0},
		{"A": "Hola", "B": 3.0},
		{"A": "Shalon", "B": 4.0},
	}
	for _, topic := range topics {
		idx := 1
		for _, item := range items {
			if err := doAppend(item, topic); err != nil {
				t.Fatalf("failed to perform append: %v", err)
			}
			fromIndex := fmt.Sprintf("%v", idx)
			//return 1 object at a time
			msgs, err := doGet(topic, genID, fromIndex)
			if err != nil {
				t.Fatalf("failed to get messages from server: %v", err)
			}
			if msgs.GenerationID != genID {
				t.Fatalf("server did not return expected generation ID: %s != %s", msgs.GenerationID, genID)
			}
			if len(msgs.Messages) != 1 {
				t.Fatalf("server did not return expected number of objects: %v != 1", len(msgs.Messages))
			}
			msg := msgs.Messages[0]
			retItem, ok := msg.Data.(map[string]interface{})
			if !ok {
				t.Fatalf("type of message did not match expected: %v != map[string]interface{}", reflect.TypeOf(retItem))
			}
			if !reflect.DeepEqual(retItem, item) {
				t.Fatalf("returned item did not match expected: %v != %v", retItem, item)
			}
			idx++
		}
	}
}

func TestE2EWatch(t *testing.T) {
	dir, err := ioutil.TempDir("", "e2e_test_")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)
	initServer(dir, t)

	genID, err := getGenerationID()
	if err != nil {
		t.Fatalf("failed to retrieve generation ID from server: %v", err)
	}

	//since we don't clean up db from other tests, make sure topics are unique
	topics := []string{
		"topicA",
		"topicB",
		"topicC",
		"topicD",
	}
	items := []map[string]interface{}{
		{"A": "Hi", "B": 0.0},
		{"A": "Hello", "B": 1.0},
		{"A": "Bonjour", "B": 2.0},
		{"A": "Hola", "B": 3.0},
		{"A": "Shalom", "B": 4.0},
	}
	for _, topic := range topics {
		msgsChan, errChan, err := initiateWatch(topic, genID, "0")
		if err != nil {
			t.Fatalf("failed to start watch: %v", err)
		}
		go func() {
			t.Fatalf("encountered error during watch: %v", <-errChan)
		}()

		receivedMessages := make(chan Message)
		go func() {
			for {
				select {
				case <-time.After(time.Second * 20):
					t.Fatalf("timed out waiting for messages to be received")
				case msgs := <-msgsChan:
					if msgs.GenerationID != genID {
						t.Fatalf("server did not return expected generation ID: %s != %s", msgs.GenerationID, genID)
					}
					for _, msg := range msgs.Messages {
						receivedMessages <- msg
					}
				}
			}
		}()

		for _, item := range items {
			if err := doAppend(item, topic); err != nil {
				t.Fatalf("failed to perform append: %v", err)
			}
			msg := <-receivedMessages
			retItem, ok := msg.Data.(map[string]interface{})
			if !ok {
				t.Fatalf("type of message did not match expected: %v != map[string]interface{}", reflect.TypeOf(retItem))
			}
			if !reflect.DeepEqual(retItem, item) {
				t.Fatalf("returned item did not match expected: %v != %v", retItem, item)
			}
		}
	}
}

func waitServerStart() error {
	timeout := time.After(time.Second * 5)
	for {

		if _, err := getGenerationID(); err == nil {
			return nil
		}
		select {
		case <-time.After(time.Millisecond * 100):
		case <-timeout:
			return fmt.Errorf("server didn't start for 5 seconds")
		}
	}
}

func getGenerationID() (string, error) {
	msgs, err := doGet("_invalid_topic_", "", "")
	if err != nil {
		return "", err
	}
	return msgs.GenerationID, nil
}

func doAppend(v interface{}, topic string) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	resp, err := doHTTPRequest("POST", "/topics/"+topic, nil, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("invalid response to HTTP POST: status %s, body: %s", resp.Status, data)
	}
	return nil
}

func doGet(topic, genID, fromIdx string) (*MessagesResponse, error) {
	query := make(url.Values)
	query.Set("generationID", genID)
	query.Set("fromIndex", fromIdx)
	resp, err := doHTTPRequest("GET", "/topics/"+topic, query, nil)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("invalid response to HTTP POST: status %s, body: %s", resp.Status, data)
	}
	var msgs MessagesResponse
	if err := json.Unmarshal(data, &msgs); err != nil {
		return nil, err
	}
	return &msgs, err
}

func initiateWatch(topic, genID, fromIdx string) (<-chan *MessagesResponse, <-chan error, error) {
	query := make(url.Values)
	query.Set("generationID", genID)
	query.Set("fromIndex", fromIdx)

	msgsChan := make(chan *MessagesResponse)
	errChan := make(chan error)
	go waitForMessages(topic, query, msgsChan, errChan)

	return msgsChan, errChan, nil
}

func waitForMessages(topic string, query url.Values, msgsChan chan *MessagesResponse, errChan chan error) {
	resp, err := doHTTPRequest("GET", "/topics/"+topic+"/watch", query, nil)
	if err != nil {
		errChan <- err
		return
	}
	reader := httputil.NewChunkedReader(resp.Body)
	dec := json.NewDecoder(reader)
	for {
		msgs := MessagesResponse{}
		err := dec.Decode(&msgs)
		if err != nil {
			errChan <- err
			return
		}
		msgsChan <- &msgs
	}
}

func doHTTPRequest(method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	u := url.URL{
		Scheme: "http",
		Host:   "localhost" + listenAddr,
		Path:   path,
	}
	if query != nil && len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}
