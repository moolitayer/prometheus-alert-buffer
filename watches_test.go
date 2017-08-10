package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

var subject = "watchManager"

type testMessageStore struct {
	messages []Message
}

func (s *testMessageStore) append(topic string, v interface{}) error {
	s.messages = append(s.messages, Message{
		Index:     uint64(len(s.messages) + 1),
		Timestamp: time.Now(),
		Data:      v,
	})
	return nil
}

func (s *testMessageStore) get(topic string, generationID string, fromIndex uint64) (*MessagesResponse, error) {
	i := int(fromIndex) - 1
	if i < 0 {
		i = 0
	}
	return &MessagesResponse{
		GenerationID: generationID,
		Messages:     s.messages[i:],
	}, nil
}

func TestWatch(t *testing.T) {
	var tests = []struct {
		context      string
		expectation  string
		messageCount int
		messageDelay time.Duration
		pushInterval time.Duration
	}{
		{
			context:      "New messages created every 1s",
			expectation:  "send messages to client every pushInterval",
			messageCount: 10,
			messageDelay: time.Millisecond,
			pushInterval: time.Millisecond * 2,
		},
	}

	for _, test := range tests {
		runWatchTest(t, test)
	}
}

func runWatchTest(t *testing.T, test struct {
	context      string
	expectation  string
	messageCount int
	messageDelay time.Duration
	pushInterval time.Duration
}) {
	t.Logf("When %s, %s should %s", test.context, subject, test.expectation)

	store := &testMessageStore{}
	r := mux.NewRouter()
	watchManager := newWatchManager(store, test.pushInterval)

	r.HandleFunc("/topics/{topic}/watch", watchManager.handleWatchRequest)
	server := httptest.NewServer(r)
	defer server.Close()
	u, _ := url.Parse(server.URL)
	u.Path = "/topics/mytopic/watch"

	messageChan := make(chan *MessagesResponse)
	go func() {
		defer close(messageChan)
		resp, err := http.Get(u.String())
		defer resp.Body.Close()
		if err != nil {
			t.Fatal(err)
			return
		}
		reader := httputil.NewChunkedReader(resp.Body)
		dec := json.NewDecoder(reader)
		for {
			msgs := MessagesResponse{}
			if err := dec.Decode(&msgs); err != nil {
				t.Fatal(err)
				return
			}
			messageChan <- &msgs
			if int(msgs.Messages[len(msgs.Messages)-1].Index) == test.messageCount {
				return
			}
		}
	}()

	submittedMessages := []string{}
	go func() {
		for i := 0; i < test.messageCount; i++ {
			item := fmt.Sprintf("{test packet #%v}", i)
			store.append("mytopic", item)
			submittedMessages = append(submittedMessages, item)
			time.Sleep(test.messageDelay)
		}
	}()

	receivedItems := 0
	for {
		select {
		case <-time.After((test.pushInterval + test.messageDelay) * time.Duration(test.messageCount)):
			t.Fatal("timed out waiting for messages to be received")
		case messagesResponse := <-messageChan:
			for _, msg := range messagesResponse.Messages {
				item := msg.Data.(string)
				if item != submittedMessages[receivedItems] {
					t.Fatalf("expected received message %s to equal sent message %s", item, submittedMessages[receivedItems])
				}
				t.Logf("Item %v == %v", item, submittedMessages[receivedItems])
				receivedItems++
				if receivedItems == test.messageCount {
					return
				}
			}
		}
	}

}
