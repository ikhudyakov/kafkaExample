package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	go StartKafka()
	time.Sleep(10 * time.Minute)
}

func StartKafka() {
	conf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test",
		GroupID:  "g1",
		MaxBytes: 10,
	}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("Message is : ", string(m.Value))
	}

}
