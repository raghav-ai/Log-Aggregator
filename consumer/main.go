package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	logsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logs_total",
		Help: "Total logs consumed, labeled by status code, method, level, and service",
	}, []string{"status_code", "method", "level", "service"})

	responseTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "response_time_seconds",
		Help:    "Response time distribution in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"level"})

	logsPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logs_processed_per_second",
		Help: "Rate of logs processed in the last second",
	})

	errorRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "error_rate",
		Help: "Ratio of 5xx responses to total requests (rolling 10s)",
	})

	dlqTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlq_messages_total",
		Help: "Total malformed messages forwarded to DLQ",
	})

	consumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Consumer lag (messages behind latest offset) per topic",
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(logsTotal, responseTimeHistogram, logsPerSecond, errorRate, dlqTotal, consumerLag)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(getEnv(key, ""))
	if err != nil || v < 1 {
		return fallback
	}
	return v
}

// bufferedLog serializes concurrent writes from the worker pool behind a large
// buffer. Unbuffered per-message encoding costs one write syscall per log, which
// alone caps throughput well below 30k/s.
type bufferedLog struct {
	mu      sync.Mutex
	buf     *bufio.Writer
	enc     *json.Encoder
	sampleN int64
	seen    int64
}

func newBufferedLog(path string) *bufferedLog {
	if err := os.MkdirAll("/app/logs", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logs dir: %v\n", err)
		os.Exit(1)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file %s: %v\n", path, err)
		os.Exit(1)
	}
	buf := bufio.NewWriterSize(f, 1024*1024)
	bl := &bufferedLog{buf: buf, enc: json.NewEncoder(buf), sampleN: 1}
	go func() {
		for range time.NewTicker(1 * time.Second).C {
			bl.mu.Lock()
			bl.buf.Flush()
			bl.mu.Unlock()
		}
	}()
	return bl
}

func (b *bufferedLog) encode(v any) {
	b.mu.Lock()
	b.enc.Encode(v)
	b.mu.Unlock()
}

// encodeSampled keeps 1-in-sampleN entries. At 30k/s, writing every log to the
// host volume is ~16GB/hour with no rotation; Kafka and Prometheus still see
// 100% of the stream, these files are for eyeballing.
func (b *bufferedLog) encodeSampled(v any) {
	if b.sampleN > 1 && atomic.AddInt64(&b.seen, 1)%b.sampleN != 0 {
		return
	}
	b.encode(v)
}

type msgWithTopic struct {
	msg   kafka.Message
	topic string
}

func startReader(broker, topic string, ch chan<- msgWithTopic, wg *sync.WaitGroup) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     "logagg-consumer",
		StartOffset: kafka.LastOffset,
		// MinBytes:1 makes the broker answer every fetch immediately with
		// whatever it has; batching fetches is far cheaper at high volume.
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       200 * time.Millisecond,
		QueueCapacity: 1000,
		// Without this, kafka-go commits the offset synchronously on every
		// ReadMessage — a broker round-trip per log, and the hard ceiling on
		// this pipeline. Batching commits trades at-most-1s of replay on
		// restart for roughly two orders of magnitude more throughput.
		CommitInterval: 1 * time.Second,
	})

	go func() {
		for range time.NewTicker(5 * time.Second).C {
			consumerLag.WithLabelValues(topic).Set(float64(reader.Stats().Lag))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer reader.Close()
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] read error: %v\n", topic, err)
				continue
			}
			ch <- msgWithTopic{msg: msg, topic: topic}
		}
	}()
}

var (
	windowTotal  int64
	windowErrors int64
	secondCount  int64
	badCount     int64
)

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	metricsPort := getEnv("METRICS_PORT", "8080")
	workers := getEnvInt("CONSUMER_WORKERS", 8)

	topics := []string{"logs.info", "logs.warn", "logs.error"}

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  "logs.dlq",
		Balancer:               &kafka.RoundRobin{},
		Async:                  true,
		BatchSize:              100,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	defer dlqWriter.Close()

	// fan-out log files, one per topic + one for DLQ. The severity files are
	// sampled; the DLQ file keeps every entry since it is low-volume and is the
	// record of what actually failed.
	fileSampleN := int64(getEnvInt("LOG_FILE_SAMPLE_N", 100))
	logFiles := map[string]*bufferedLog{
		"logs.info":  newBufferedLog("/app/logs/info.log"),
		"logs.warn":  newBufferedLog("/app/logs/warn.log"),
		"logs.error": newBufferedLog("/app/logs/error.log"),
		"logs.dlq":   newBufferedLog("/app/logs/dlq.log"),
	}
	for _, topic := range topics {
		logFiles[topic].sampleN = fileSampleN
	}

	msgCh := make(chan msgWithTopic, 10000)
	dlqCh := make(chan kafka.Message, 10000)
	var wg sync.WaitGroup
	for _, topic := range topics {
		startReader(broker, topic, msgCh, &wg)
	}

	go func() {
		for msg := range dlqCh {
			if err := dlqWriter.WriteMessages(context.Background(), msg); err != nil {
				fmt.Fprintf(os.Stderr, "dlq write error: %v\n", err)
			}
		}
	}()

	go func() {
		for range time.NewTicker(10 * time.Second).C {
			total := atomic.SwapInt64(&windowTotal, 0)
			errors := atomic.SwapInt64(&windowErrors, 0)
			if total > 0 {
				errorRate.Set(float64(errors) / float64(total))
			}
		}
	}()

	go func() {
		for range time.NewTicker(1 * time.Second).C {
			logsPerSecond.Set(float64(atomic.SwapInt64(&secondCount, 0)))
		}
	}()

	// At 30k/s the ~1% malformed stream is ~300 msg/s; logging each one to
	// stderr floods the Docker log driver, so report a periodic count instead.
	go func() {
		for range time.NewTicker(10 * time.Second).C {
			if n := atomic.SwapInt64(&badCount, 0); n > 0 {
				fmt.Fprintf(os.Stderr, "[consumer] %d malformed messages routed to DLQ in last 10s\n", n)
			}
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		fmt.Fprintf(os.Stderr, "Metrics server listening on :%s\n", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			fmt.Fprintf(os.Stderr, "metrics server error: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Fprintf(os.Stderr, "Consumer started: broker=%s topics=%v workers=%d\n", broker, topics, workers)

	validLevels := map[string]bool{"info": true, "warn": true, "error": true}

	sendToDLQ := func(item msgWithTopic, reason string) {
		dlqTotal.Inc()
		atomic.AddInt64(&badCount, 1)
		logFiles["logs.dlq"].encode(map[string]string{
			"origin": item.topic,
			"raw":    string(item.msg.Value),
			"error":  reason,
		})
		// Carry headers through: the DLQ consumer's retry counter lives there,
		// and dropping them would reset the count on every cycle.
		dlqCh <- kafka.Message{
			Key:     []byte(item.topic),
			Value:   item.msg.Value,
			Headers: item.msg.Headers,
		}
	}

	var workerWg sync.WaitGroup
	for w := 0; w < workers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for item := range msgCh {
				var entry LogEntry
				if err := json.Unmarshal(item.msg.Value, &entry); err != nil {
					sendToDLQ(item, err.Error())
					continue
				}

				if !validLevels[entry.Level] || entry.Service == "" || entry.StatusCode == 0 {
					reason := fmt.Sprintf("missing required fields: level=%q service=%q status_code=%d", entry.Level, entry.Service, entry.StatusCode)
					sendToDLQ(item, reason)
					continue
				}

				statusStr := strconv.Itoa(entry.StatusCode)
				logsTotal.WithLabelValues(statusStr, entry.Method, entry.Level, entry.Service).Inc()
				responseTimeHistogram.WithLabelValues(entry.Level).Observe(float64(entry.ResponseTimeMs) / 1000.0)

				atomic.AddInt64(&secondCount, 1)
				atomic.AddInt64(&windowTotal, 1)
				if entry.StatusCode >= 500 {
					atomic.AddInt64(&windowErrors, 1)
				}

				logFiles["logs."+entry.Level].encodeSampled(entry)
			}
		}()
	}

	workerWg.Wait()
}
