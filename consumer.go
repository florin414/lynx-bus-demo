package main

import (
	"context"
	"fmt"
	"log"
	"time"

	lynxbus "github.com/florin414/lynx-bus/api"
)

const pollAttempts = 6
const pollRetryDelay = 120 * time.Millisecond
const pollTimeout = 1 * time.Second

type consumerConfigMutator func(*lynxbus.ConsumerConfig)

func consumerRun(brokerAddr string, topics []string, mutator consumerConfigMutator) (*lynxbus.Consumer, error) {
	cfg := lynxbus.ConsumerConfig{
		Brokers: []string{brokerAddr},
		Topics:  topics,
	}
	if mutator != nil {
		mutator(&cfg)
	}

	consumer, err := lynxbus.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("consumer create for topics %v: %w", topics, err)
	}
	log.Printf("[consumer] connected broker=%s topics=%v", brokerAddr, topics)
	return consumer, nil
}

func closeConsumer(label string, consumer *lynxbus.Consumer) {
	if consumer == nil {
		return
	}
	if err := consumer.Close(); err != nil {
		logError("consumer close "+label, err)
	}
}

func consumeMessages(consumer *lynxbus.Consumer, topics []string, maxMessages int) ([]lynxbus.FetchedMessage, error) {
	if err := consumer.Subscribe(topics); err != nil {
		return nil, fmt.Errorf("subscribe topics %v: %w", topics, err)
	}
	return pollWithRetry(consumer, topics, maxMessages)
}

func consumeOnce(brokerAddr string, topics []string, maxMessages int, scope string, mutator consumerConfigMutator) ([]lynxbus.FetchedMessage, error) {
	consumer, err := consumerRun(brokerAddr, topics, mutator)
	if err != nil {
		return nil, err
	}
	defer closeConsumer(scope, consumer)

	messages, err := consumeMessages(consumer, topics, maxMessages)
	if err != nil {
		return nil, err
	}
	logFetchedMessages(scope, messages)
	return messages, nil
}

func pollWithRetry(consumer *lynxbus.Consumer, topics []string, maxMessages int) ([]lynxbus.FetchedMessage, error) {
	var lastErr error
	for attempt := 1; attempt <= pollAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), pollTimeout)
		messages, err := consumer.Poll(ctx, maxMessages)
		cancel()

		if err != nil {
			lastErr = err
			time.Sleep(pollRetryDelay)
			continue
		}
		if len(messages) > 0 {
			return messages, nil
		}

		time.Sleep(pollRetryDelay)
	}

	if lastErr != nil {
		return nil, fmt.Errorf("poll topics %v: %w", topics, lastErr)
	}
	return nil, fmt.Errorf("poll topics %v: no messages received after %d attempts", topics, pollAttempts)
}
