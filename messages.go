package main

import "time"

// A MessagesResponse contains a sequence of messages for a given generation ID.
type MessagesResponse struct {
	GenerationID string    `json:"generationID"`
	Messages     []Message `json:"messages"`
}

// A Message models a message with its data and a sequential index that is valid
// within a given generation ID.
type Message struct {
	Index     uint64      `json:"index"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}
