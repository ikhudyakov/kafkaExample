package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

var Address = []string{"localhost:9092"}

type Message struct {
	Text (string)
}

func main() {
	go consumer(Address)
	time.Sleep(10 * time.Minute)
}

func consumer(address []string) {
	conf := kafka.ReaderConfig{
		Brokers:  address,
		Topic:    "test",
		GroupID:  "g1",
		MaxBytes: 10,
	}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println(err)
			continue
		}
		var network bytes.Buffer
		_, err = network.Write(m.Value)
		if err != nil {
			log.Println(err)
			continue
		}
		dec := gob.NewDecoder(&network)
		var message Message
		err = dec.Decode(&message)
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("Message is : %s, offset = %d", message, m.Offset)
	}

}
