package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

var Address = []string{"localhost:9092"}

func main() {
	syncProducer(Address)
	asyncProducer(Address)
}

// синхронный режим сообщений
func syncProducer(address []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second

	p, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer p.Close()

	topic := "test"
	srcValue := "sync: this is a message.  index = %d  time: %s"
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf(srcValue, i, time.Now().Format("15:04:05"))
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}
		part, offset, err := p.SendMessage(msg)
		if err != nil {
			log.Printf("send message(%s) err=%s \n", value, err)
		} else {
			fmt.Fprintf(os.Stdout, "%s Успешно отправлено, part = %d, offset = %d \n", value, part, offset)
		}
		time.Sleep(2 * time.Second)
	}
}

// асинхронный режим сообщений
func asyncProducer(address []string) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(address, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer producer.AsyncClose()

	go func(p sarama.AsyncProducer) {
		for {
			select {
			case suc := <-p.Successes():
				fmt.Fprintf(os.Stdout, "%s Успешно отправлено, part = %d, offset = %d \n", suc.Value, suc.Partition, suc.Offset)
			case fail := <-p.Errors():
				fmt.Println("err: ", fail.Err)
			}
		}
	}(producer)

	var value string
	for i := 0; ; i++ {
		time.Sleep(500 * time.Millisecond)
		value = fmt.Sprintf("async: this is a message.  index = %d  time: %s", i, time.Now().Format("15:04:05"))

		msg := &sarama.ProducerMessage{
			Topic: "test",
		}

		msg.Value = sarama.ByteEncoder(value)

		producer.Input() <- msg
		time.Sleep(2 * time.Second)
	}
}
