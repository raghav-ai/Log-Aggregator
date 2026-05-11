package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
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

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	metricsPort := getEnv("METRICS_PORT", "8080")

	topics := []string{"logs.info", "logs.warn", "logs.error"}

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  "logs.dlq",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer dlqWriter.Close()

	// fan-out log files, one per topic + one for DLQ
	logFiles := map[string]*json.Encoder{
		"logs.info":  json.NewEncoder(openLogFile("/app/logs/info.log")),
		"logs.warn":  json.NewEncoder(openLogFile("/app/logs/warn.log")),
		"logs.error": json.NewEncoder(openLogFile("/app/logs/error.log")),
		"logs.dlq":   json.NewEncoder(openLogFile("/app/logs/dlq.log")),
	}

	msgCh := make(chan msgWithTopic, 1000)
	dlqCh := make(chan kafka.Message, 1000)
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

	var windowTotal, windowErrors, secondCount int64

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

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		fmt.Fprintf(os.Stderr, "Metrics server listening on :%s\n", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			fmt.Fprintf(os.Stderr, "metrics server error: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Fprintf(os.Stderr, "Consumer started: broker=%s topics=%v\n", broker, topics)

	validLevels := map[string]bool{"info": true, "warn": true, "error": true}

	sendToDLQ := func(item msgWithTopic, reason string) {
		dlqTotal.Inc()
		logFiles["logs.dlq"].Encode(map[string]string{
			"origin": item.topic,
			"raw":    string(item.msg.Value),
			"error":  reason,
		})
		dlqCh <- kafka.Message{
			Key:   []byte(item.topic),
			Value: item.msg.Value,
		}
	}

	for item := range msgCh {
		var entry LogEntry
		if err := json.Unmarshal(item.msg.Value, &entry); err != nil {
			fmt.Fprintf(os.Stderr, "parse error on %s: %v\n", item.topic, err)
			sendToDLQ(item, err.Error())
			continue
		}

		if !validLevels[entry.Level] || entry.Service == "" || entry.StatusCode == 0 {
			reason := fmt.Sprintf("missing required fields: level=%q service=%q status_code=%d", entry.Level, entry.Service, entry.StatusCode)
			fmt.Fprintf(os.Stderr, "invalid entry on %s: %s\n", item.topic, reason)
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

		logFiles["logs."+entry.Level].Encode(entry)
	}
}
