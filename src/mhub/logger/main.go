package main

import (
        "fmt"
	"unicode/utf8"
	"os"
	"github.com/Shopify/sarama"
)

func main() {
	client, err := sarama.NewClient("a_logger_for_mhub", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	} else {
		os.Stderr.WriteString("> connected\n")
	}
	defer client.Close()

	consumer, err := sarama.NewConsumer(client, "received", 0, "", nil)
	if err != nil {
		panic(err)
	} else {
		os.Stderr.WriteString("> consumer ready\n")
	}
	defer consumer.Close()

	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			fmt.Println(utf8.FullRune(event.Value))
		}
	}
}
