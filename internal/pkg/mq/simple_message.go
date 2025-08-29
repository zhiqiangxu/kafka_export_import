package mq

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type SimpleMessage struct {
	Partition int32
	Data      []byte
}

func (msg *SimpleMessage) Marshal() ([]byte, error) {
	return json.Marshal(msg)
}

func (msg *SimpleMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, msg)
}

func (msg *SimpleMessage) ToKafkaMessage(topic string) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: msg.Partition,
		},
		Value: msg.Data,
	}
}
