package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	port := getEnv("INGEST_PORT", "8081")

	writers := map[string]*kafka.Writer{
		"info":  {Addr: kafka.TCP(broker), Topic: "logs.info", Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true},
		"warn":  {Addr: kafka.TCP(broker), Topic: "logs.warn", Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true},
		"error": {Addr: kafka.TCP(broker), Topic: "logs.error", Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true},
	}
	for _, w := range writers {
		defer w.Close()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var entry LogEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			http.Error(w, `{"error":"invalid JSON: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}

		if entry.Timestamp == "" {
			entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}
		if entry.Level == "" {
			switch {
			case entry.StatusCode >= 500:
				entry.Level = "error"
			case entry.StatusCode >= 400:
				entry.Level = "warn"
			default:
				entry.Level = "info"
			}
		}
		if _, ok := writers[entry.Level]; !ok {
			entry.Level = "info"
		}

		data, err := json.Marshal(entry)
		if err != nil {
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}

		if err := writers[entry.Level].WriteMessages(context.Background(), kafka.Message{Value: data}); err != nil {
			fmt.Fprintf(os.Stderr, "ingest write error: %v\n", err)
			http.Error(w, `{"error":"failed to publish to Kafka"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status":"accepted","topic":"logs.%s"}`, entry.Level)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	})

	fmt.Printf("Ingest API started on :%s — POST /ingest to publish logs\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
