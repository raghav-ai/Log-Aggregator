package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
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

var (
	methods     = []string{"GET", "POST", "PUT", "DELETE"}
	paths       = []string{"/api/users", "/api/orders", "/api/products", "/health", "/api/auth"}
	statusCodes = []int{200, 200, 200, 200, 301, 404, 404, 500}
	services    = []string{"auth-service", "order-service", "product-service", "user-service", "gateway"}
)

func randomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}

func severity(statusCode int) string {
	switch {
	case statusCode >= 500:
		return "error"
	case statusCode >= 400:
		return "warn"
	default:
		return "info"
	}
}

func generateLog() LogEntry {
	statusCode := statusCodes[rand.Intn(len(statusCodes))]
	return LogEntry{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		Level:          severity(statusCode),
		Service:        services[rand.Intn(len(services))],
		IP:             randomIP(),
		Method:         methods[rand.Intn(len(methods))],
		Path:           paths[rand.Intn(len(paths))],
		StatusCode:     statusCode,
		ResponseTimeMs: rand.Intn(500) + 10,
	}
}

var malformedMessages = [][]byte{
	[]byte(`not json at all`),
	[]byte(`{"level":"info","service":"broken-service"}`),
	[]byte(`{"timestamp":"now","level":42,"status_code":"OK","response_time_ms":"fast"}`),
	[]byte(`{}`),
	[]byte(`<13>May 8 host sshd[1234]: invalid user admin`),
}

func generateMalformed() []byte {
	return malformedMessages[rand.Intn(len(malformedMessages))]
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func waitForKafka(broker string) {
	fmt.Fprintf(os.Stderr, "Waiting for Kafka at %s...\n", broker)
	for {
		conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Fprintln(os.Stderr, "Kafka is ready.")
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

func openLogFile(path string) *os.File {
	if err := os.MkdirAll("/app/logs", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logs dir: %v\n", err)
		os.Exit(1)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file %s: %v\n", path, err)
		os.Exit(1)
	}
	return f
}

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	rateMs, _ := strconv.Atoi(getEnv("LOG_RATE_MS", "200"))

	waitForKafka(broker)

	writers := map[string]*kafka.Writer{
		"info":  newWriter(broker, "logs.info"),
		"warn":  newWriter(broker, "logs.warn"),
		"error": newWriter(broker, "logs.error"),
	}
	for _, w := range writers {
		defer w.Close()
	}

	logFile := openLogFile("/app/logs/producer.log")
	defer logFile.Close()
	encoder := json.NewEncoder(logFile)

	fmt.Fprintf(os.Stderr, "Producer started: broker=%s rate=%dms routing to logs.{info,warn,error}\n", broker, rateMs)

	levels := []string{"info", "warn", "error"}

	for {
		// ~1% of messages are malformed to exercise the DLQ
		if rand.Intn(100) < 1 {
			topic := levels[rand.Intn(len(levels))]
			bad := generateMalformed()
			if err := writers[topic].WriteMessages(context.Background(), kafka.Message{Value: bad}); err != nil {
				fmt.Fprintf(os.Stderr, "write error (malformed): %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "sent malformed message to logs.%s: %s\n", topic, bad)
			}
		} else {
			entry := generateLog()
			data, err := json.Marshal(entry)
			if err != nil {
				fmt.Fprintf(os.Stderr, "marshal error: %v\n", err)
				continue
			}
			if err := writers[entry.Level].WriteMessages(context.Background(), kafka.Message{Value: data}); err != nil {
				fmt.Fprintf(os.Stderr, "write error: %v\n", err)
			} else {
				encoder.Encode(entry)
			}
		}

		time.Sleep(time.Duration(rateMs) * time.Millisecond)
	}
}
