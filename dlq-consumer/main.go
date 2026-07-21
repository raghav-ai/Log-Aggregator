package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type LogEntry struct {
	Timestamp      string `json:"timestamp"`
	Level          string `json:"level"`
	Service        string `json:"service"`
	IP             string `json:"ip"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	StatusCode     int    `json:"status_code"`
	ResponseTimeMs int    `json:"response_time_ms"`
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var validLevels = map[string]bool{"info": true, "warn": true, "error": true}

// retryable must stay in lockstep with the consumer's validation. If this check
// is weaker, any message in the gap is forwarded, rejected, and returned to the
// DLQ forever — a loop that saturates the broker at high volume.
func retryable(entry LogEntry) bool {
	return validLevels[entry.Level] && entry.Service != "" && entry.StatusCode != 0
}

// maxRetries caps redelivery for messages that keep failing for reasons the
// validation above cannot see, so nothing can cycle indefinitely.
const maxRetries = 3

var retried, dropped, exhausted int64

func retryCount(msg kafka.Message) int {
	for _, h := range msg.Headers {
		if h.Key == "x-retry-count" {
			n, err := strconv.Atoi(string(h.Value))
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")

	newRetryWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Topic:                  topic,
			Balancer:               &kafka.RoundRobin{},
			Async:                  true,
			BatchSize:              100,
			BatchTimeout:           10 * time.Millisecond,
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
		}
	}
	retryWriters := map[string]*kafka.Writer{
		"info":  newRetryWriter("logs.info"),
		"warn":  newRetryWriter("logs.warn"),
		"error": newRetryWriter("logs.error"),
	}
	for _, w := range retryWriters {
		defer w.Close()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "logs.dlq",
		GroupID:     "logagg-dlq-consumer",
		StartOffset: kafka.LastOffset,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		// Same synchronous-commit ceiling as the main consumer; the DLQ carries
		// ~1% of 30k/s, which is still far too much for a commit per message.
		CommitInterval: 1 * time.Second,
	})
	defer reader.Close()

	fmt.Printf("DLQ consumer started: broker=%s topic=logs.dlq\n", broker)

	// Per-message stdout would be hundreds of lines/s at full rate; summarize
	// on a ticker and keep the detail in the consumer's dlq.log.
	go func() {
		for range time.NewTicker(10 * time.Second).C {
			r := atomic.SwapInt64(&retried, 0)
			d := atomic.SwapInt64(&dropped, 0)
			e := atomic.SwapInt64(&exhausted, 0)
			if r > 0 || d > 0 || e > 0 {
				fmt.Printf("[dlq] last 10s: %d retried, %d permanently malformed, %d retries exhausted\n", r, d, e)
			}
		}
	}()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "dlq read error: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		var entry LogEntry
		if err := json.Unmarshal(msg.Value, &entry); err != nil {
			// permanently unparseable — count and skip (human intervention required)
			atomic.AddInt64(&dropped, 1)
			continue
		}

		// Parseable but still invalid: retrying would only get it rejected
		// again, so treat it as permanently malformed rather than recycling it.
		if !retryable(entry) {
			atomic.AddInt64(&dropped, 1)
			continue
		}

		attempts := retryCount(msg)
		if attempts >= maxRetries {
			atomic.AddInt64(&exhausted, 1)
			continue
		}

		// message is now parseable — forward to the appropriate severity topic
		level := entry.Level
		if _, ok := retryWriters[level]; !ok {
			level = "info"
		}
		out := kafka.Message{
			Value:   msg.Value,
			Headers: []kafka.Header{{Key: "x-retry-count", Value: []byte(strconv.Itoa(attempts + 1))}},
		}
		if err := retryWriters[level].WriteMessages(context.Background(), out); err != nil {
			fmt.Fprintf(os.Stderr, "[dlq] retry write to logs.%s failed: %v\n", level, err)
		} else {
			atomic.AddInt64(&retried, 1)
		}
	}
}
