package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var ctx = context.Background()

var (
	pingsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sentinel_pings_total",
			Help: "Total number of pings executed",
		},
		[]string{"status"},
	)
	pingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "sentinel_ping_latency_ms",
			Help:    "Latency of pings in milliseconds",
			Buckets: []float64{100, 250, 500, 1000, 2500, 5000},
		},
	)
)

func init() {
	prometheus.MustRegister(pingsTotal)
	prometheus.MustRegister(pingLatency)
}

type MonitorTask struct {
	ID  int    `json:"id"`
	URL string `json:"url"`
}

type InternalResult struct {
	MonitorID  int
	StatusCode int
	Latency    int
	Status     string
	Delivery   amqp.Delivery
}

func main() {
	dbConnStr := "postgres://AfshanQ:Afshan525@127.0.0.1:5432/sre_db?sslmode=disable"
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer db.Close()

	amqpConn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer amqpConn.Close()

	ch, err := amqpConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("monitor_tasks_dead", true, false, false, false, nil)
	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "monitor_tasks_dead",
	}

	q, err := ch.QueueDeclare("monitor_tasks", true, false, false, false, args)

	err = ch.Qos(100, 0, false)

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:16379",
	})

	resultsChan := make(chan InternalResult, 100)

	go func() {
		var resultBuffer []InternalResult
		batchSize := 10

		for res := range resultsChan {
			resultBuffer = append(resultBuffer, res)

			if len(resultBuffer) >= batchSize {
				valueStrings := make([]string, 0, len(resultBuffer))
				valueArgs := make([]interface{}, 0, len(resultBuffer)*4)

				for i, r := range resultBuffer {
					pos := i * 4
					valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d)", pos+1, pos+2, pos+3, pos+4))
					valueArgs = append(valueArgs, r.MonitorID, r.StatusCode, r.Latency, r.Status)
				}

				bulkQuery := fmt.Sprintf("INSERT INTO checks (monitor_id, status_code, latency_ms, status_text) VALUES %s", strings.Join(valueStrings, ","))
				_, err := db.Exec(bulkQuery, valueArgs...)

				if err == nil {
					for _, r := range resultBuffer {
						r.Delivery.Ack(false)
					}
					log.Printf("Batch of %d saved to DB and ACKed", len(resultBuffer))
				} else {
					for _, r := range resultBuffer {
						r.Delivery.Nack(false, true)
					}
					log.Printf("Batch failed, messages requeued: %v", err)
				}
				resultBuffer = nil
			}
		}
	}()

	go func() {
		for d := range msgs {
			go func(delivery amqp.Delivery) {
				var task MonitorTask
				if err := json.Unmarshal(delivery.Body, &task); err != nil {
					delivery.Nack(false, false)
					return
				}

				var statusCode int
				var statusText string
				var latencyMs int
				success := false

				for i := 0; i < 3; i++ {
					start := time.Now()
					client := http.Client{Timeout: 5 * time.Second}
					resp, err := client.Get(task.URL)

					if err == nil && resp.StatusCode < 400 {
						latencyMs = int(time.Since(start).Milliseconds())
						statusCode = resp.StatusCode
						statusText = "UP"
						success = true
						resp.Body.Close()
						break
					}
					time.Sleep(time.Duration(1<<(i+1)) * time.Second)
				}

				if !success {
					statusText = "DOWN"
				}

				db.Exec(`UPDATE monitors SET last_checked = NOW(), status = $1 WHERE id = $2`, statusText, task.ID)
				rdb.Set(ctx, fmt.Sprintf("monitor:%d:status", task.ID), statusText, 2*time.Hour)
				pingsTotal.WithLabelValues(statusText).Inc()
				pingLatency.Observe(float64(latencyMs))

				resultsChan <- InternalResult{
					MonitorID:  task.ID,
					StatusCode: statusCode,
					Latency:    latencyMs,
					Status:     statusText,
					Delivery:   delivery,
				}
			}(d)
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()

	log.Printf("Guardian Worker Online | QoS: 100 | Mode: Concurrent")
	select {}
}
