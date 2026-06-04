package main

import (
	"fmt"
	"log"
	"strings"

	lynxbus "github.com/florin414/lynx-bus/api"
)

func brokerRun(cfg runtimeConfig) (*lynxbus.Broker, string) {
	
	observer := newLoggingObserver()

	broker, err := lynxbus.NewBroker(lynxbus.BrokerConfig{
		ListenAddr:      cfg.listenAddr,
		ConnectionLimit: cfg.connectionLimit,
		AcceptDeadline:  cfg.acceptDeadline,
		SessionClassify: newLoggingClassifier(),
		SessionObserver: observer,
		Storage: lynxbus.StorageConfig{
			DataDir: cfg.dataDir,
		},
	})

	if err != nil {
		logFatal("broker create", err)
	}

	if err := broker.Start(); err != nil {
		logFatal("broker start", err)
	}

	addr := broker.Addr().String()
	log.Printf("[broker] listening on %s (max connections: %d, accept-deadline: %s, data-dir: %s)", addr, cfg.connectionLimit, cfg.acceptDeadline, cfg.dataDir)
	return broker, addr
}

func ensureTopic(broker *lynxbus.Broker, topic string, partitions int32) error {
	return ensureTopicRF(broker, topic, partitions, 1)
}

func ensureTopicRF(broker *lynxbus.Broker, topic string, partitions int32, replicationFactor int32) error {
	err := broker.CreateTopic(topic, partitions, replicationFactor)
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "exist") {
		return nil
	}
	return fmt.Errorf("create topic %q: %w", topic, err)
}
