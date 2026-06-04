package main

import (
	"fmt"
	"log"
	"time"

	lynxbus "github.com/florin414/lynx-bus/api"
)

type messageRecord struct {
	Key   string
	Value string
}

type producerConfigMutator func(*lynxbus.ProducerConfig)

func producerRun(brokerAddr, topic string, mutator producerConfigMutator) (*lynxbus.Producer, error) {
	cfg := lynxbus.ProducerConfig{
		Brokers:      []string{brokerAddr},
		Topic:        topic,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}
	if mutator != nil {
		mutator(&cfg)
	}

	producer, err := lynxbus.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("producer create for topic %q: %w", topic, err)
	}

	log.Printf("[producer] connected broker=%s topic=%q", brokerAddr, topic)
	return producer, nil
}

func publishStringMessage(producer *lynxbus.Producer, key, value string) error {
	msg := lynxbus.NewStringMessage(key, value)
	if err := producer.Publish(msg.Key, msg.Value); err != nil {
		return fmt.Errorf("publish key=%q: %w", key, err)
	}
	log.Printf("[producer] published key=%q value=%q", key, value)
	return nil
}

func closeProducer(label string, producer *lynxbus.Producer) {
	if producer == nil {
		return
	}
	if err := producer.Close(); err != nil {
		logError("producer close "+label, err)
	}
}

func publishRecords(producer *lynxbus.Producer, records []messageRecord) error {
	for _, record := range records {
		if err := publishStringMessage(producer, record.Key, record.Value); err != nil {
			return err
		}
	}
	return nil
}

func publishToTopic(brokerAddr, topic string, records []messageRecord, mutator producerConfigMutator) error {
	producer, err := producerRun(brokerAddr, topic, mutator)
	if err != nil {
		return err
	}
	defer closeProducer(topic, producer)

	return publishRecords(producer, records)
}
