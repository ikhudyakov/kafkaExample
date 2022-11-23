package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

var Address = []string{"localhost:9092"}

type Message struct {
	Text (string)
}

func main() {
	producer(Address)
}

func producer(address []string) {
	conf := kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	}

	writer := kafka.NewWriter(conf)
	srcValue := "sync: this is a message. index = %d  time: %s"

	for i := 0; i < 10; i++ {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		value := fmt.Sprintf(srcValue, i, time.Now().Format("15:04:05"))
		err := enc.Encode(&Message{Text: value})
		if err != nil {
			log.Fatal(err)
		}

		err = writer.WriteMessages(context.Background(),
			kafka.Message{Value: buf.Bytes()},
		)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Fprintf(os.Stdout, "%s Успешно отправлено\n", value)
		}
		time.Sleep(2 * time.Second)
	}

	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}
}
