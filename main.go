package main

import (
	"context"
	"log"
)

func main() {
	setupLogging()
	cfg := resolveRuntimeConfig()

	broker, brokerAddr := brokerRun(cfg)
	defer func() {
		log.Println("[main] shutting down broker...")
		if err := broker.Shutdown(context.Background()); err != nil {
			logError("broker shutdown", err)
		}
	}()

	log.Println("[main] starting full demo — all concepts...")
	if err := flowFullDemo(broker, brokerAddr); err != nil {
		logFatal("demo", err)
	}

	logBrokerTopics(broker)
	log.Println("[main] demo completed successfully")
}
