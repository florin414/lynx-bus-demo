package main

import (
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	lynxbus "github.com/florin414/lynx-bus/api"
)

type loggingObserver struct {
	starts  atomic.Int32
	stops   atomic.Int32
	active  atomic.Int32
	timings sync.Map // session ID -> start time

	mu             sync.Mutex
	clientSequence map[string]int // client IP -> connection count (for inferring purpose)
}

func newLoggingObserver() *loggingObserver {
	return &loggingObserver{
		clientSequence: make(map[string]int),
	}
}

func connInfo(c net.Conn) (clientAddr, network string) {
	clientAddr = "unknown"
	network = "unknown"
	if c == nil {
		return
	}
	if addr := c.RemoteAddr(); addr != nil {
		clientAddr = addr.String()
		network = addr.Network()
	}
	return
}

func (o *loggingObserver) inferPurpose(clientIP string) string {
	o.mu.Lock()
	o.clientSequence[clientIP]++
	seq := o.clientSequence[clientIP]
	o.mu.Unlock()

	// Producer SDK connection sequence: 1=api-versions, 2=metadata, 3=produce
	switch seq {
	case 1:
		return "api-versions (broker discovery)"
	case 2:
		return "metadata (topic resolution)"
	default:
		return fmt.Sprintf("produce (message write #%d)", seq-2)
	}
}

func (o *loggingObserver) OnSessionStart(s *lynxbus.Session) {
	o.starts.Add(1)
	o.active.Add(1)
	o.timings.Store(s.ID, time.Now())

	clientAddr, network := connInfo(s.Conn)
	clientIP, _, _ := net.SplitHostPort(clientAddr)
	purpose := o.inferPurpose(clientIP)

	log.Printf("  [observer] ✓ session start   id=%-3v  role=%-8v  client=%-21s  purpose=%q  (active=%d  total=%d)",
		s.ID, s.Role, clientAddr+" "+network, purpose, o.active.Load(), o.starts.Load())
}

func (o *loggingObserver) OnSessionStop(s *lynxbus.Session) {
	o.stops.Add(1)
	o.active.Add(-1)

	duration := "unknown"
	if started, ok := o.timings.LoadAndDelete(s.ID); ok {
		duration = time.Since(started.(time.Time)).Round(time.Microsecond).String()
	}

	clientAddr, _ := connInfo(s.Conn)

	log.Printf("  [observer] · session stop    id=%-3v  role=%-8v  client=%-21s  duration=%-10s  (active=%d)",
		s.ID, s.Role, clientAddr, duration, o.active.Load())
}

func (o *loggingObserver) OnClassifyError(s *lynxbus.Session, err error) {
	clientAddr, network := connInfo(s.Conn)
	log.Printf("  [observer] ✗ classify error  id=%v  conn=%s  client=%s  err=%v", s.ID, network, clientAddr, err)
}

func (o *loggingObserver) OnAcceptError(err error) {
	log.Printf("  [observer] ✗ accept error    err=%v", err)
}

func newLoggingClassifier() lynxbus.SessionClassifier {
	return func(conn net.Conn) (lynxbus.SessionRole, error) {
		role, err := lynxbus.DefaultClassifier(conn)
		if err != nil {
			log.Printf("  [classifier] ✗ error  conn=%-21s  err=%v", conn.RemoteAddr(), err)
			return role, err
		}
		log.Printf("  [classifier] ✓ role=%-8v  conn=%s", role, conn.RemoteAddr())
		return role, nil
	}
}

func logFetchedMessages(scope string, messages []lynxbus.FetchedMessage) {
	if len(messages) == 0 {
		log.Printf("[%s] no messages fetched", scope)
		return
	}
	for _, msg := range messages {
		log.Printf("[%s] topic=%q partition=%d offset=%d key=%q value=%q",
			scope, msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}
}

func logBrokerTopics(broker *lynxbus.Broker) {
	snapshot := broker.TopicSnapshot()
	if len(snapshot) == 0 {
		log.Println("[storage] no topics stored")
		return
	}

	topics := make([]string, 0, len(snapshot))
	for topic := range snapshot {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		messageCount := snapshot[topic]
		log.Printf("[storage] topic=%q messages=%d", topic, messageCount)
	}
}

func logTopicStorage(broker *lynxbus.Broker, topic string) {
	meta, err := broker.GetTopicMetadata(topic)
	if err != nil {
		logError("topic metadata lookup", err)
		return
	}
	log.Printf("[storage] data saved  topic=%q partitions=%d replication_factor=%d leaders=%v",
		meta.Name, meta.NumPartitions, meta.ReplicationFactor, meta.PartitionLeaders)
}

func logError(context string, err error) {
	log.Printf("[error] %s: %v", context, err)
}

func logFatal(context string, err error) {
	log.Fatalf("[fatal] %s: %v", context, err)
}
