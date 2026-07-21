package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
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

var (
	methods     = []string{"GET", "POST", "PUT", "DELETE"}
	paths       = []string{"/api/users", "/api/orders", "/api/products", "/health", "/api/auth"}
	statusCodes = []int{200, 200, 200, 200, 301, 404, 404, 500}
	services    = []string{"auth-service", "order-service", "product-service", "user-service", "gateway"}
)

// Each worker paces itself on this tick, emitting (rate / workers * tickInterval)
// messages per tick. Short enough to stay smooth, long enough that timer overhead
// stays negligible at 30k/s.
const tickInterval = 10 * time.Millisecond

func randomIP(r *rand.Rand) string {
	return fmt.Sprintf("%d.%d.%d.%d", r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256))
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

func generateLog(r *rand.Rand, now string) LogEntry {
	statusCode := statusCodes[r.Intn(len(statusCodes))]
	return LogEntry{
		Timestamp:      now,
		Level:          severity(statusCode),
		Service:        services[r.Intn(len(services))],
		IP:             randomIP(r),
		Method:         methods[r.Intn(len(methods))],
		Path:           paths[r.Intn(len(paths))],
		StatusCode:     statusCode,
		ResponseTimeMs: r.Intn(500) + 10,
	}
}

var malformedMessages = [][]byte{
	[]byte(`not json at all`),
	[]byte(`{"level":"info","service":"broken-service"}`),
	[]byte(`{"timestamp":"now","level":42,"status_code":"OK","response_time_ms":"fast"}`),
	[]byte(`{}`),
	[]byte(`<13>May 8 host sshd[1234]: invalid user admin`),
}

func generateMalformed(r *rand.Rand) []byte {
	return malformedMessages[r.Intn(len(malformedMessages))]
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(getEnv(key, ""))
	if err != nil || v < 0 {
		return fallback
	}
	return v
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

var (
	sentCount  int64
	errorCount int64
)

func newWriter(broker, topic string, batchSize int) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.RoundRobin{},
		// Async batching is what makes 30k/s reachable: WriteMessages queues
		// instead of blocking on a broker round-trip per call.
		Async:                  true,
		BatchSize:              batchSize,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		Completion: func(msgs []kafka.Message, err error) {
			if err != nil {
				atomic.AddInt64(&errorCount, int64(len(msgs)))
				return
			}
			atomic.AddInt64(&sentCount, int64(len(msgs)))
		},
	}
}

// sampledLog appends a fraction of produced entries to producer.log. At 30k/s
// writing every entry would be several MB/s of unrotated disk churn, so we keep
// a sample for spot-checking and let Kafka carry the real stream.
type sampledLog struct {
	mu  sync.Mutex
	buf *bufio.Writer
	enc *json.Encoder
}

func newSampledLog(path string) *sampledLog {
	if err := os.MkdirAll("/app/logs", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logs dir: %v\n", err)
		os.Exit(1)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file %s: %v\n", path, err)
		os.Exit(1)
	}
	buf := bufio.NewWriterSize(f, 256*1024)
	sl := &sampledLog{buf: buf, enc: json.NewEncoder(buf)}
	go func() {
		for range time.NewTicker(1 * time.Second).C {
			sl.mu.Lock()
			sl.buf.Flush()
			sl.mu.Unlock()
		}
	}()
	return sl
}

func (s *sampledLog) write(entry LogEntry) {
	s.mu.Lock()
	s.enc.Encode(entry)
	s.mu.Unlock()
}

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	targetRate := getEnvInt("LOG_RATE_PER_SEC", 30000)
	workers := getEnvInt("PRODUCER_WORKERS", 8)
	batchSize := getEnvInt("PRODUCER_BATCH_SIZE", 500)
	sampleN := getEnvInt("LOG_SAMPLE_N", 1000)
	if workers < 1 {
		workers = 1
	}
	if sampleN < 1 {
		sampleN = 1
	}

	waitForKafka(broker)

	writers := map[string]*kafka.Writer{
		"info":  newWriter(broker, "logs.info", batchSize),
		"warn":  newWriter(broker, "logs.warn", batchSize),
		"error": newWriter(broker, "logs.error", batchSize),
	}
	for _, w := range writers {
		defer w.Close()
	}

	logFile := newSampledLog("/app/logs/producer.log")

	fmt.Fprintf(os.Stderr, "Producer started: broker=%s target=%d msg/s workers=%d batch=%d sample=1/%d\n",
		broker, targetRate, workers, batchSize, sampleN)

	levels := []string{"info", "warn", "error"}
	ratePerWorker := float64(targetRate) / float64(workers)
	perTick := ratePerWorker * tickInterval.Seconds()

	for w := 0; w < workers; w++ {
		go func(seed int64) {
			r := rand.New(rand.NewSource(seed))
			ticker := time.NewTicker(tickInterval)
			defer ticker.Stop()

			// Fractional carry so a per-tick budget like 37.5 averages out
			// to the exact target rate instead of truncating to 37.
			var budget float64
			for range ticker.C {
				budget += perTick
				n := int(budget)
				budget -= float64(n)

				now := time.Now().UTC().Format(time.RFC3339)
				for i := 0; i < n; i++ {
					// ~1% of messages are malformed to exercise the DLQ
					if r.Intn(100) < 1 {
						topic := levels[r.Intn(len(levels))]
						bad := generateMalformed(r)
						if err := writers[topic].WriteMessages(context.Background(), kafka.Message{Value: bad}); err != nil {
							atomic.AddInt64(&errorCount, 1)
						}
						continue
					}

					entry := generateLog(r, now)
					data, err := json.Marshal(entry)
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						continue
					}
					if err := writers[entry.Level].WriteMessages(context.Background(), kafka.Message{Value: data}); err != nil {
						atomic.AddInt64(&errorCount, 1)
						continue
					}
					if r.Intn(sampleN) == 0 {
						logFile.write(entry)
					}
				}
			}
		}(time.Now().UnixNano() + int64(w))
	}

	// Report actual achieved throughput so the target is verifiable.
	for range time.NewTicker(5 * time.Second).C {
		sent := atomic.SwapInt64(&sentCount, 0)
		errs := atomic.SwapInt64(&errorCount, 0)
		fmt.Fprintf(os.Stderr, "[producer] %d msg/s acked (errors: %d)\n", sent/5, errs)
	}
}
