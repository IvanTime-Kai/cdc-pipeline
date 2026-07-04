package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/segmentio/kafka-go"
)

type CDCEvent struct {
	Payload struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
		Op     string         `json:"op"` // c=create, u=update, d=delete
	} `json:"payload"`
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "ecommerce.public.products",
		GroupID: "es-indexer",
	})

	defer r.Close()

	log.Println("consumer start, waiting for messages")

	for {
		msg, err := r.ReadMessage(context.Background())

		if err != nil {
			log.Printf("read err: %v", err)
			break
		}

		var event CDCEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("parse error: %v", err)
			continue
		}

		if err := handleEvent(event); err != nil {
			log.Printf("handle err: %v", err)
		}
	}
}

func handleEvent(event CDCEvent) error {
	op := event.Payload.Op
	switch op {
	case "c", "u": // create hoặc update
		doc := event.Payload.After
		id, _ := doc["id"].(string)

		if doc["status"] == "deleted" {
			return deleteDocument(id)
		}

		return indexDocument(id, doc)
	case "d": // delete
		doc := event.Payload.Before
		id, _ := doc["id"].(string)
		return deleteDocument(id)
	default:
		log.Printf("unknown op: %s", op)
		return nil
	}
}

func indexDocument(id string, doc map[string]any) error {
	body, _ := json.Marshal(doc)
	url := fmt.Sprintf("http://localhost:9200/products/_doc/%s", id)

	req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(string(body)))
	req.Header.Set("Content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	log.Printf("indexed doc id=%s status=%d", id, resp.StatusCode)
	return nil
}

func deleteDocument(id string) error {
	url := fmt.Sprintf("http://localhost:9200/products/_doc/%s", id)

	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("deleted doc id=%s status=%d", id, resp.StatusCode)
	return nil
}
