package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	lynxbus "github.com/florin414/lynx-bus/api"
)

// compressionGzip mirrors core.CompressionGzip — not re-exported from api package
const compressionGzip = 1

const (
	topicMessaging     = "messaging"
	topicPageView      = "activity-pageview"
	topicSearch        = "activity-search"
	topicPurchase      = "activity-purchase"
	topicMetrics       = "metrics"
	topicLogs          = "logs"
	topicOrdersRaw     = "orders-raw"
	topicOrdersOut     = "orders-processed"
	topicAccountEvents = "account-events"
)

type accountEvent struct {
	Type   string `json:"type"`
	Amount int    `json:"amount"`
}

func demoSection(step int, title string) {
	content := fmt.Sprintf("  STEP %d  ·  %s  ", step, title)
	border := strings.Repeat("═", len([]rune(content)))
	log.Printf("")
	log.Printf("[step] ╔%s╗", border)
	log.Printf("[step] ║%s║", content)
	log.Printf("[step] ╚%s╝", border)
	log.Printf("")
}

func demoSubSection(label string) {
	log.Printf("[demo] ▸ %s", label)
}

func flowFullDemo(broker *lynxbus.Broker, brokerAddr string) error {

	// ------------------------------------------------------------------ //
	// STEP 1: Messaging — Fire-and-Forget + Point-to-Point + Topic Admin
	// ------------------------------------------------------------------ //
	demoSection(1, "Messaging — Fire-and-Forget, Point-to-Point, Topic Admin, AcceptDeadline")

	demoSubSection("Topic Admin — CreateTopic · GetTopicMetadata · GetPartitionLeader · ElectLeader")

	topicsBefore := broker.ListTopics()
	sort.Strings(topicsBefore)
	log.Printf("[demo-messaging] topics before: %v", topicsBefore)

	if err := ensureTopic(broker, topicMessaging, 2); err != nil {
		return err
	}

	meta, err := broker.GetTopicMetadata(topicMessaging)
	if err != nil {
		return fmt.Errorf("get metadata %q: %w", topicMessaging, err)
	}
	log.Printf("[demo-messaging] ✓ topic=%q  partitions=%d  replication_factor=%d  leaders=%v",
		meta.Name, meta.NumPartitions, meta.ReplicationFactor, meta.PartitionLeaders)

	leader, err := broker.GetPartitionLeader(topicMessaging, 0)
	if err != nil {
		return fmt.Errorf("get partition leader: %w", err)
	}
	if err := broker.ElectLeader(topicMessaging, 0, leader); err != nil {
		return fmt.Errorf("elect leader: %w", err)
	}
	log.Printf("[demo-messaging] ✓ leader elected  partition=0  leader_node=%d", leader)

	demoSubSection("Fire-and-Forget — publish without consumer, message persisted on disk at rest")

	// Fire-and-Forget: publish without consumer, message durably stored
	if err := publishToTopic(brokerAddr, topicMessaging, []messageRecord{
		{Key: "faf-1", Value: `{"msg":"fire and forget — durably stored, no consumer needed"}`},
	}, nil); err != nil {
		return err
	}
	logTopicStorage(broker, topicMessaging)
	log.Printf("[demo-messaging] ✓ message at rest — no consumer required, readable at any future time")

	demoSubSection("Point-to-Point — ProducerConfig(RequiredAcks=1, DialTimeout=3s) → ConsumerConfig(GroupID, DialTimeout=3s)")

	// Point-to-Point: single producer → single consumer
	// DialTimeout=3s: max time to establish TCP connection to broker
	// RequiredAcks=1: leader must acknowledge before producer continues
	if err := publishToTopic(brokerAddr, topicMessaging, []messageRecord{
		{Key: "order-100", Value: `{"event":"created","amount":250}`},
		{Key: "order-101", Value: `{"event":"confirmed","amount":180}`},
		{Key: "order-102", Value: `{"event":"shipped","amount":99}`},
	}, func(cfg *lynxbus.ProducerConfig) {
		cfg.DialTimeout = 3 * time.Second
		cfg.RequiredAcks = 1
	}); err != nil {
		return err
	}
	// GroupID="messaging-group": consumer group identifier for coordinated consumption
	// DialTimeout=3s: max time to establish TCP connection to broker
	p2pMessages, err := consumeOnce(brokerAddr, []string{topicMessaging}, 20, "messaging-consumer",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "messaging-group"
			cfg.DialTimeout = 3 * time.Second
		})
	if err != nil {
		return err
	}
	log.Printf("[demo-messaging] ✓ %d messages delivered to consumer group %q", len(p2pMessages), "messaging-group")

	demoSubSection("AcceptDeadline — BrokerConfig(AcceptDeadline=300ms, SessionClassify, SessionObserver)")

	// AcceptDeadline: broker-level config — stops accepting new TCP connections after duration.
	// Demonstrated on an isolated broker so the main broker remains available for remaining steps.
	// AcceptDeadline=300ms: publish before expiry (ok), sleep 400ms, publish after (blocked).
	deadlineBroker, err := lynxbus.NewBroker(lynxbus.BrokerConfig{
		ListenAddr:      "127.0.0.1:0",
		ConnectionLimit: 10,
		AcceptDeadline:  300 * time.Millisecond,
		SessionClassify: newLoggingClassifier(),
		SessionObserver: newLoggingObserver(),
		Storage:         lynxbus.StorageConfig{DataDir: "output/deadline-broker"},
	})
	if err != nil {
		return fmt.Errorf("deadline broker create: %w", err)
	}
	if err := deadlineBroker.Start(); err != nil {
		return fmt.Errorf("deadline broker start: %w", err)
	}
	defer func() { _ = deadlineBroker.Shutdown(context.Background()) }()

	deadlineAddr := deadlineBroker.Addr().String()
	log.Printf("[demo-deadline] broker listening at %s — accepting connections for 300ms only", deadlineAddr)

	if err := ensureTopic(deadlineBroker, "deadline-test", 1); err != nil {
		return err
	}
	if err := publishToTopic(deadlineAddr, "deadline-test", []messageRecord{
		{Key: "before", Value: `{"window":"open"}`},
	}, nil); err != nil {
		return fmt.Errorf("publish before deadline: %w", err)
	}
	log.Printf("[demo-deadline] ✓ publish BEFORE deadline — connection accepted (window still open)")

	log.Printf("[demo-deadline] sleeping 400ms — letting AcceptDeadline of 300ms expire...")
	time.Sleep(400 * time.Millisecond)

	log.Printf("[demo-deadline] attempting publish AFTER AcceptDeadline expired...")
	err = publishToTopic(deadlineAddr, "deadline-test", []messageRecord{
		{Key: "after", Value: `{"window":"closed"}`},
	}, func(cfg *lynxbus.ProducerConfig) {
		cfg.MaxRetries = 1
		cfg.RetryBackoff = 50 * time.Millisecond
	})
	if err != nil {
		log.Printf("[demo-deadline] ✗ publish AFTER deadline — connection rejected as expected: %v", err)
	} else {
		log.Printf("[demo-deadline] publish after deadline — OS TCP backlog accepted (within kernel buffer)")
	}
	log.Printf("[demo-deadline] ✓ AcceptDeadline verified — broker stopped accepting after configured duration")

	// ------------------------------------------------------------------ //
	// STEP 2: Website Activity Tracking — one topic per activity type
	// ------------------------------------------------------------------ //
	demoSection(2, "Website Activity Tracking — multi-topic, key routing, CompressionGzip, FetchMaxBytes")

	demoSubSection("3 topics · 3 users · CompressionGzip — pageviews, searches, purchases")
	log.Printf("[demo-activity] topic routing: key=userID → same partition → order guaranteed per user")

	activityTopics := []struct {
		name  string
		parts int32
	}{
		{topicPageView, 2},
		{topicSearch, 2},
		{topicPurchase, 1},
	}
	for _, t := range activityTopics {
		if err := ensureTopic(broker, t.name, t.parts); err != nil {
			return err
		}
	}

	// CompressionGzip: reduces payload size for high-volume activity data
	activityMutator := func(cfg *lynxbus.ProducerConfig) {
		cfg.CompressionType = compressionGzip
	}

	users := []string{"user-A", "user-B", "user-C"}
	pages := []string{"/home", "/products", "/cart", "/checkout"}
	queries := []string{"laptop", "phone", "headphones"}

	for _, u := range users {
		for _, page := range pages {
			if err := publishToTopic(brokerAddr, topicPageView, []messageRecord{
				{Key: u, Value: fmt.Sprintf(`{"user":%q,"page":%q,"ts":%d}`, u, page, time.Now().UnixMilli())},
			}, activityMutator); err != nil {
				return err
			}
		}
		for _, q := range queries {
			if err := publishToTopic(brokerAddr, topicSearch, []messageRecord{
				{Key: u, Value: fmt.Sprintf(`{"user":%q,"query":%q}`, u, q)},
			}, activityMutator); err != nil {
				return err
			}
		}
		if err := publishToTopic(brokerAddr, topicPurchase, []messageRecord{
			{Key: u, Value: fmt.Sprintf(`{"user":%q,"item":"product-X","price":99}`, u)},
		}, activityMutator); err != nil {
			return err
		}
	}

	log.Printf("[demo-activity] ✓ published: %d pageviews · %d searches · %d purchases  (CompressionGzip, key=userID)",
		len(users)*len(pages), len(users)*len(queries), len(users))

	demoSubSection("Consuming — ConsumerConfig(FetchMaxBytes=512KB, GroupID per topic)")

	// FetchMaxBytes=512KB: larger fetch buffer for high-volume activity streams
	pvMessages, err := consumeOnce(brokerAddr, []string{topicPageView}, 40, "activity-pageview-consumer",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "activity-pageview-group"
			cfg.FetchMaxBytes = 512 * 1024
		})
	if err != nil {
		return err
	}
	pvPerUser := map[string]int{}
	for _, m := range pvMessages {
		pvPerUser[string(m.Key)]++
	}
	log.Printf("[demo-activity] ✓ pageviews per user: %v  (key routing preserved order per user)", pvPerUser)

	srMessages, err := consumeOnce(brokerAddr, []string{topicSearch}, 30, "activity-search-consumer",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "activity-search-group"
			cfg.FetchMaxBytes = 512 * 1024
		})
	if err != nil {
		return err
	}
	log.Printf("[demo-activity] ✓ search events fetched: %d", len(srMessages))

	purchaseMessages, err := consumeOnce(brokerAddr, []string{topicPurchase}, 20, "activity-purchase-consumer",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "activity-purchase-group"
		})
	if err != nil {
		return err
	}
	log.Printf("[demo-activity] ✓ purchase events fetched: %d", len(purchaseMessages))

	// ------------------------------------------------------------------ //
	// STEP 3: Metrics + Log Aggregation — Fan-In with producer batching
	// ------------------------------------------------------------------ //
	demoSection(3, "Metrics + Log Aggregation — Fan-In, BatchSize, LingerMs, FetchMaxWait")

	if err := ensureTopic(broker, topicMetrics, 2); err != nil {
		return err
	}
	if err := ensureTopic(broker, topicLogs, 1); err != nil {
		return err
	}

	demoSubSection("Fan-In — 3 services → metrics (BatchSize=128, LingerMs=60ms, MaxRetries=3, RequestTimeout=5s)")

	batchMutator := func(cfg *lynxbus.ProducerConfig) {
		cfg.BatchSize = 128
		cfg.LingerMs = 60
		cfg.MaxRetries = 3
		cfg.RetryBackoff = 100 * time.Millisecond
		cfg.RequestTimeout = 5 * time.Second
	}

	services := []struct{ name, metric, level, msg string }{
		{"auth-service", `{"cpu":72.5,"mem_mb":512}`, "INFO", "authentication successful"},
		{"billing-service", `{"cpu":64.2,"mem_mb":420}`, "WARN", "payment retry #2"},
		{"gateway-service", `{"cpu":55.0,"mem_mb":310}`, "ERROR", "upstream timeout"},
	}
	for _, svc := range services {
		if err := publishToTopic(brokerAddr, topicMetrics, []messageRecord{
			{Key: svc.name, Value: svc.metric},
		}, batchMutator); err != nil {
			return err
		}
		if err := publishToTopic(brokerAddr, topicLogs, []messageRecord{
			{Key: svc.name, Value: fmt.Sprintf(`{"service":%q,"level":%q,"msg":%q}`, svc.name, svc.level, svc.msg)},
		}, nil); err != nil {
			return err
		}
	}

	log.Printf("[demo-metrics] ✓ 3 services published metrics + logs  (batching: flush at 128 bytes OR 60ms)")

	demoSubSection("Aggregating — ConsumerConfig(FetchMaxWait=300ms) for low-latency monitoring")

	// FetchMaxWait=300ms: operational monitoring needs fast results, don't wait long
	metricsMessages, err := consumeOnce(brokerAddr, []string{topicMetrics}, 30, "metrics-aggregator",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "metrics-group"
			cfg.FetchMaxWait = 300 * time.Millisecond
		})
	if err != nil {
		return err
	}
	perService := map[string]int{}
	for _, m := range metricsMessages {
		perService[string(m.Key)]++
	}
	log.Printf("[demo-metrics] ✓ metrics per service: %v", perService)

	logsMessages, err := consumeOnce(brokerAddr, []string{topicLogs}, 20, "log-aggregator",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "logs-group"
			cfg.FetchMaxWait = 200 * time.Millisecond
		})
	if err != nil {
		return err
	}
	log.Printf("[demo-logs] ✓ %d log entries aggregated from %d services  (fan-in complete)", len(logsMessages), len(services))

	// ------------------------------------------------------------------ //
	// STEP 4: Stream Processing — consume → transform → re-publish
	// ------------------------------------------------------------------ //
	demoSection(4, "Stream Processing — orders-raw → transform → orders-processed, SessionTimeout, MaxRetries")

	if err := ensureTopic(broker, topicOrdersRaw, 1); err != nil {
		return err
	}
	if err := ensureTopic(broker, topicOrdersOut, 1); err != nil {
		return err
	}

	demoSubSection("Pipeline: orders-raw → [consume → uppercase + enrich with timestamp] → orders-processed")

	if err := publishToTopic(brokerAddr, topicOrdersRaw, []messageRecord{
		{Key: "ord-1", Value: "order-created"},
		{Key: "ord-2", Value: "order-confirmed"},
		{Key: "ord-3", Value: "order-packed"},
		{Key: "ord-4", Value: "order-shipped"},
	}, nil); err != nil {
		return err
	}

	demoSubSection("ConsumerConfig(SessionTimeout=10s) — fast failure detection for stream processors")

	// SessionTimeout=10s: stream processor declares itself dead quickly if it stops heartbeating
	streamConsumer, err := consumerRun(brokerAddr, []string{topicOrdersRaw},
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "stream-processor"
			cfg.SessionTimeout = 10 * time.Second
		})
	if err != nil {
		return err
	}
	defer closeConsumer("stream-processor", streamConsumer)

	rawMessages, err := consumeMessages(streamConsumer, []string{topicOrdersRaw}, 20)
	if err != nil {
		return err
	}
	logFetchedMessages("stream-input", rawMessages)

	demoSubSection("ProducerConfig(MaxRetries=5, RetryBackoff=200ms) — resilient re-publish to output topic")

	streamOutputProducer, err := producerRun(brokerAddr, topicOrdersOut, func(cfg *lynxbus.ProducerConfig) {
		cfg.MaxRetries = 5
		cfg.RetryBackoff = 200 * time.Millisecond
	})
	if err != nil {
		return err
	}
	defer closeProducer("stream-output", streamOutputProducer)

	for _, msg := range rawMessages {
		enriched := fmt.Sprintf(`{"event":%q,"processed_at":%d}`,
			strings.ToUpper(string(msg.Value)), time.Now().UnixMilli())
		if err := publishStringMessage(streamOutputProducer, string(msg.Key), enriched); err != nil {
			return err
		}
	}
	log.Printf("[stream-output] ✓ %d orders transformed: uppercased + enriched with processed_at timestamp", len(rawMessages))

	demoSubSection("Verifying processed output on orders-processed topic")
	processedMessages, err := consumeOnce(brokerAddr, []string{topicOrdersOut}, 20, "stream-output-consumer",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "stream-output-group"
		})
	if err != nil {
		return err
	}
	log.Printf("[stream-output] ✓ %d processed orders confirmed on output topic", len(processedMessages))

	// ------------------------------------------------------------------ //
	// STEP 5: AI Inference Pipeline — Event Sourcing, RF=3, leader per
	//         partition, leader failover, RequiredAcks=-1, replay
	// ------------------------------------------------------------------ //
	demoSection(5, "AI Inference Pipeline — RF=3, leader per partition, failover, RequiredAcks=-1, replay")

	demoSubSection("CreateTopic(partitions=3, RF=3) — 1 leader + 2 replici per partitie")

	// replicationFactor=3: fiecare partitie are 1 leader activ si 2 follower replici
	// RequiredAcks=-1: producatorul asteapta confirmarea de la TOATE replica-urile ISR
	// — durabilitate maxima, zero pierdere de date pentru AI inference logs
	if err := ensureTopicRF(broker, topicAccountEvents, 3, 3); err != nil {
		return err
	}

	meta5, err := broker.GetTopicMetadata(topicAccountEvents)
	if err != nil {
		return fmt.Errorf("get metadata %q: %w", topicAccountEvents, err)
	}
	log.Printf("[demo-ai] ✓ topic=%q  partitions=%d  replication_factor=%d",
		meta5.Name, meta5.NumPartitions, meta5.ReplicationFactor)

	// Afiseaza liderul curent pentru fiecare partitie — fiecare partitie are propriul lider
	for p := int32(0); p < meta5.NumPartitions; p++ {
		ldr, err := broker.GetPartitionLeader(topicAccountEvents, p)
		if err != nil {
			return fmt.Errorf("get leader partition %d: %w", p, err)
		}
		r1, r2 := (ldr+1)%3, (ldr+2)%3
		log.Printf("[demo-ai] partition=%d  leader=node-%d  followers=[node-%d, node-%d]  (RF=3: 1 leader + 2 replici)", p, ldr, r1, r2)
	}

	demoSubSection("Leader Failover — node-0 pica, un follower preia leadership-ul pentru partition-0")

	// Simuleaza failover: liderul curent al partition-0 e inlocuit cu un follower
	// In productie, coordonatorul detecteaza ca node-0 nu mai raspunde la heartbeat
	// si declanseaza alegerea unui nou lider din ISR (in-sync replicas)
	leaderBefore, err := broker.GetPartitionLeader(topicAccountEvents, 0)
	if err != nil {
		return fmt.Errorf("get leader before failover: %w", err)
	}
	log.Printf("[demo-ai] leader curent partition=0: node-%d  — simulam ca node-%d pica", leaderBefore, leaderBefore)

	// follower-ul node-(leaderBefore+1) este ales ca noul lider
	newLeader := leaderBefore + 1
	if err := broker.ElectLeader(topicAccountEvents, 0, newLeader); err != nil {
		return fmt.Errorf("elect new leader: %w", err)
	}

	leaderAfter, err := broker.GetPartitionLeader(topicAccountEvents, 0)
	if err != nil {
		return fmt.Errorf("get leader after failover: %w", err)
	}
	log.Printf("[demo-ai] ✓ failover complet: node-%d a picat → node-%d preia leadership-ul  (partition=0 continua fara intrerupere)", leaderBefore, leaderAfter)

	demoSubSection("ProducerConfig(RequiredAcks=-1) — publish AI inference events, toate replicile confirma")

	events := []messageRecord{
		{Key: "model-gpt", Value: `{"type":"inference","amount":1024}`},
		{Key: "model-gpt", Value: `{"type":"inference","amount":768}`},
		{Key: "model-bert", Value: `{"type":"inference","amount":512}`},
		{Key: "model-gpt", Value: `{"type":"inference","amount":2048}`},
		{Key: "model-bert", Value: `{"type":"inference","amount":384}`},
	}
	if err := publishToTopic(brokerAddr, topicAccountEvents, events, func(cfg *lynxbus.ProducerConfig) {
		cfg.RequiredAcks = -1 // toate replica-urile ISR confirma inainte de ack — zero pierdere
	}); err != nil {
		return err
	}
	log.Printf("[demo-ai] ✓ %d inference events scrise  (RequiredAcks=-1: confirmat de toate replicile)", len(events))

	demoSubSection("Replay — ConsumerConfig(FetchMinBytes=64, FetchMaxBytes=256KB) — reconstruct total tokens")

	// FetchMinBytes=64: asteapta minim 64 bytes inainte de a returna — garanteaza batch complet
	// FetchMaxBytes=256KB: suficient pentru replay AI events fara over-fetching
	replayed, err := consumeOnce(brokerAddr, []string{topicAccountEvents}, 30, "event-sourcing-replay",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "event-sourcing-group"
			cfg.FetchMinBytes = 64
			cfg.FetchMaxBytes = 256 * 1024
		})
	if err != nil {
		return err
	}

	totalTokens := 0
	for _, msg := range replayed {
		var event accountEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("decode ai event: %w", err)
		}
		if event.Type == "inference" {
			totalTokens += event.Amount
		}
	}
	log.Printf("[demo-ai] ✓ replayed %d inference events  →  total tokens procesati = %d  (expected 4736)", len(replayed), totalTokens)
	log.Printf("[demo-ai]   breakdown: gpt(1024+768+2048) + bert(512+384) = %d", totalTokens)

	demoSubSection("Commit Log — verifying monotonic offsets (append-only guarantee)")

	// Verifica monotonie per-partitie — fiecare partitie are propria secventa de offset-uri
	lastOffsetPerPartition := map[int32]int64{}
	for _, msg := range replayed {
		if prev, seen := lastOffsetPerPartition[msg.Partition]; seen {
			if msg.Offset != prev+1 {
				return fmt.Errorf("offsets not monotonic partition=%d (%d → %d)", msg.Partition, prev, msg.Offset)
			}
		}
		lastOffsetPerPartition[msg.Partition] = msg.Offset
	}
	for part, last := range lastOffsetPerPartition {
		log.Printf("[demo-commitlog] ✓ partition=%d  offsets 0..%d — monotonic, append-only log integrity confirmed", part, last)
	}
	logTopicStorage(broker, topicAccountEvents)

	demoSubSection("SessionSnapshot + TopicSnapshot — inspect active sessions and stored messages")

	snap := broker.SessionSnapshot()
	log.Printf("[demo-snapshot] ✓ sessions  producers=%d  consumers=%d  total=%d",
		snap.Producers, snap.Consumers, snap.Total)
	logBrokerTopics(broker)

	// ------------------------------------------------------------------ //
	// STEP 6: Cleanup — DeleteTopic, verify final topic list
	// ------------------------------------------------------------------ //
	demoSection(6, "Cleanup — DeleteTopic, verify final topic list")

	allTopics := []string{
		topicMessaging,
		topicPageView, topicSearch, topicPurchase,
		topicMetrics, topicLogs,
		topicOrdersRaw, topicOrdersOut,
		topicAccountEvents,
	}

	demoSubSection(fmt.Sprintf("DeleteTopic × %d topics → verify broker is empty", len(allTopics)))

	for _, t := range allTopics {
		if err := broker.DeleteTopic(t); err != nil {
			log.Printf("[demo-cleanup] ✗ delete topic=%q err=%v", t, err)
		} else {
			log.Printf("[demo-cleanup] ✓ deleted topic=%q", t)
		}
	}

	topicsAfter := broker.ListTopics()
	sort.Strings(topicsAfter)
	if len(topicsAfter) == 0 {
		log.Printf("[demo-cleanup] ✓ all topics removed — broker storage is clean")
	} else {
		log.Printf("[demo-cleanup] topics remaining: %v", topicsAfter)
	}

	return nil
}

// flowStreamProcessingDemo runs only the Stream Processing scenario:
// consume from orders-raw → transform → re-publish to orders-processed.
func flowStreamProcessingDemo(broker *lynxbus.Broker, brokerAddr string) error {
	demoSection(1, "Stream Processing — orders-raw → transform → orders-processed, SessionTimeout, MaxRetries")

	if err := ensureTopic(broker, topicOrdersRaw, 1); err != nil {
		return err
	}
	if err := ensureTopic(broker, topicOrdersOut, 1); err != nil {
		return err
	}

	demoSubSection("Pipeline: orders-raw → [consume → uppercase + enrich with timestamp] → orders-processed")

	if err := publishToTopic(brokerAddr, topicOrdersRaw, []messageRecord{
		{Key: "ord-1", Value: "order-created"},
		{Key: "ord-2", Value: "order-confirmed"},
		{Key: "ord-3", Value: "order-packed"},
		{Key: "ord-4", Value: "order-shipped"},
	}, nil); err != nil {
		return err
	}

	demoSubSection("ConsumerConfig(SessionTimeout=10s) — fast failure detection for stream processors")

	// SessionTimeout=10s: stream processor declares itself dead quickly if it stops heartbeating
	streamConsumer, err := consumerRun(brokerAddr, []string{topicOrdersRaw},
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "stream-processor"
			cfg.SessionTimeout = 10 * time.Second
		})
	if err != nil {
		return err
	}
	defer closeConsumer("stream-processor", streamConsumer)

	rawMessages, err := consumeMessages(streamConsumer, []string{topicOrdersRaw}, 20)
	if err != nil {
		return err
	}
	logFetchedMessages("stream-input", rawMessages)

	demoSubSection("ProducerConfig(MaxRetries=5, RetryBackoff=200ms) — resilient re-publish to output topic")

	streamOutputProducer, err := producerRun(brokerAddr, topicOrdersOut, func(cfg *lynxbus.ProducerConfig) {
		cfg.MaxRetries = 5
		cfg.RetryBackoff = 200 * time.Millisecond
	})
	if err != nil {
		return err
	}
	defer closeProducer("stream-output", streamOutputProducer)

	for _, msg := range rawMessages {
		enriched := fmt.Sprintf(`{"event":%q,"processed_at":%d}`,
			strings.ToUpper(string(msg.Value)), time.Now().UnixMilli())
		if err := publishStringMessage(streamOutputProducer, string(msg.Key), enriched); err != nil {
			return err
		}
	}
	log.Printf("[stream-output] ✓ %d orders transformed: uppercased + enriched with processed_at timestamp", len(rawMessages))

	demoSubSection("Verifying processed output on orders-processed topic")
	processedMessages, err := consumeOnce(brokerAddr, []string{topicOrdersOut}, 20, "stream-output-consumer",
		func(cfg *lynxbus.ConsumerConfig) {
			cfg.GroupID = "stream-output-group"
		})
	if err != nil {
		return err
	}
	log.Printf("[stream-output] ✓ %d processed orders confirmed on output topic", len(processedMessages))

	return nil
}
