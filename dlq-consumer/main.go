package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")

	retryWriters := map[string]*kafka.Writer{
		"info":  {Addr: kafka.TCP(broker), Topic: "logs.info", Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true},
		"warn":  {Addr: kafka.TCP(broker), Topic: "logs.warn", Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true},
		"error": {Addr: kafka.TCP(broker), Topic: "logs.error", Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true},
	}
	for _, w := range retryWriters {
		defer w.Close()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "logs.dlq",
		GroupID:     "logagg-dlq-consumer",
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
	})
	defer reader.Close()

	fmt.Printf("DLQ consumer started: broker=%s topic=logs.dlq\n", broker)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "dlq read error: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		origTopic := string(msg.Key)
		fmt.Printf("[dlq] received (origin=%s): %s\n", origTopic, string(msg.Value))

		var entry LogEntry
		if err := json.Unmarshal(msg.Value, &entry); err != nil {
			// permanently unparseable — log and skip (human intervention required)
			fmt.Fprintf(os.Stderr, "[dlq] permanently malformed message (origin=%s): %s\n", origTopic, string(msg.Value))
			continue
		}

		// message is now parseable — forward to the appropriate severity topic
		level := entry.Level
		if _, ok := retryWriters[level]; !ok {
			level = "info"
		}
		if err := retryWriters[level].WriteMessages(context.Background(), kafka.Message{Value: msg.Value}); err != nil {
			fmt.Fprintf(os.Stderr, "[dlq] retry write to logs.%s failed: %v\n", level, err)
		} else {
			fmt.Printf("[dlq] retried message to logs.%s\n", level)
		}
	}
}
