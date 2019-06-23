package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	inputData := readDataFromFile()
	produceKafkaMessages(inputData)

	outputData := consumeKafkaMessages()
	writeKafkaMessageToFile(outputData)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func readDataFromFile() [][]byte {
	inputData := make([][]byte, 7)

	// Open file in ReadOnly mode
	file, err := os.OpenFile("inputFile.txt", os.O_RDONLY, 0777)
	checkErr(err)

	i := 0
	reader := bufio.NewReader(file)
	for {
		bytes, err := reader.ReadBytes(byte('\n'))
		if err == io.EOF {
			break
		}
		inputData[i] = bytes
		i++
		if i >= 50{
			panic("Index out of bounds")
		}
	}

	file.Close()
	return inputData
}

func produceKafkaMessages(inputData [][]byte) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})

	checkErr(err)

	// Publish messages to Kafka
	topic := "my-replicated-topic"
	for _, message := range inputData {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          message,
		}, nil)
	}

	counter := 1
	// Handle Producer Events
	for e := range producer.Events() {
		switch event := e.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", event.TopicPartition)
			} else {
				if counter == len(inputData){
					producer.Flush(15 * 1000)
					print("Closing Producer\n")
					producer.Close()
					print("Producer closed\n")
					return
				}
				fmt.Printf("Delivered message to %v\n", event.TopicPartition)
				counter++
			}
		}
	}
}

func consumeKafkaMessages() [][]byte {

	outputData := make([][]byte, 7)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "test-consumer-group",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
	})

	checkErr(err)

	consumer.SubscribeTopics([]string{"my-replicated-topic"}, nil)

	index := 0
	for {
		msg, err := consumer.ReadMessage(5 * time.Second)
		if index >= len(outputData){
			print("Closing Consumer\n")
			consumer.Close()
			print("Consumer closed\n")
			return outputData
		}
		checkErr(err)
		//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		outputData[index] = msg.Value
		index++
	}
}

func writeKafkaMessageToFile(outputData [][]byte) {

	file, err := os.OpenFile("OutputFile.txt", os.O_APPEND|os.O_WRONLY, 0777)

	checkErr(err)

	for _, data  := range outputData {
		file.WriteString(time.Now().Format(time.Stamp) + "\n")
		_, err := file.Write(data)
		checkErr(err)
		file.WriteString("\n\n")
		checkErr(err)

		fmt.Printf("Wrote message: %v", string(data))
	}

	file.Close()
}
