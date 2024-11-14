// cmd/loadtest/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Config struct {
	QueueURL       string
	Duration       time.Duration
	BaseRate       int
	BurstRate      int
	BurstDuration  time.Duration
	BurstInterval  time.Duration
	MessageSize    int
	Concurrent     int
	RequestTimeout time.Duration // Added timeout config
}

type MessagePayload struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Size      int       `json:"size"`
	Data      string    `json:"data"`
}

type Stats struct {
	messagesSent  atomic.Int64
	messagesError atomic.Int64
	totalLatency  atomic.Int64 // in milliseconds
	maxLatency    atomic.Int64 // in milliseconds
}

func main() {
	cfg := parseFlags()

	// Load AWS configuration
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("Unable to load SDK config: %v", err)
	}

	client := sqs.NewFromConfig(awsCfg)

	// Create root context with test duration
	rootCtx, rootCancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer rootCancel()

	// Setup graceful shutdown
	shutdownCh := make(chan struct{})
	go handleShutdown(rootCancel, shutdownCh)

	// Initialize stats
	stats := &Stats{}

	// Start load generators
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			generateLoad(rootCtx, client, cfg, stats, id)
		}(i)
	}

	// Start stats reporter
	go reportStats(rootCtx, stats)

	// Wait for completion or shutdown
	wg.Wait()
	close(shutdownCh)

	// Final stats
	log.Printf("Load test completed - Final Statistics:")
	log.Printf("Total messages sent: %d", stats.messagesSent.Load())
	log.Printf("Total errors: %d", stats.messagesError.Load())
	if sent := stats.messagesSent.Load(); sent > 0 {
		avgLatency := time.Duration(stats.totalLatency.Load()/sent) * time.Millisecond
		maxLatency := time.Duration(stats.maxLatency.Load()) * time.Millisecond
		log.Printf("Average latency: %v", avgLatency)
		log.Printf("Maximum latency: %v", maxLatency)
	}
}

func generateLoad(ctx context.Context, client *sqs.Client, cfg Config, stats *Stats, workerID int) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	burstTicker := time.NewTicker(cfg.BurstInterval)
	defer burstTicker.Stop()

	isBurst := false
	burstEnd := time.Time{}

	log.Printf("Worker %d started", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", workerID)
			return

		case <-burstTicker.C:
			isBurst = true
			burstEnd = time.Now().Add(cfg.BurstDuration)
			log.Printf("Worker %d starting burst", workerID)

		case <-ticker.C:
			// Determine current rate
			rate := cfg.BaseRate
			if isBurst && time.Now().Before(burstEnd) {
				rate = cfg.BurstRate
			} else {
				isBurst = false
			}

			// Calculate messages to send this second
			msgsPerWorker := rate / cfg.Concurrent
			if workerID < rate%cfg.Concurrent {
				msgsPerWorker++
			}

			// Send messages with individual timeouts
			for i := 0; i < msgsPerWorker; i++ {
				// Create context with timeout for individual request
				reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
				err := sendMessage(reqCtx, client, cfg, stats)
				cancel() // Clean up request context

				if err != nil {
					stats.messagesError.Add(1)
					if ctx.Err() == nil { // Only log if main context isn't cancelled
						log.Printf("Worker %d error: %v", workerID, err)
					}
				}
			}
		}
	}
}

func sendMessage(ctx context.Context, client *sqs.Client, cfg Config, stats *Stats) error {
	payload := MessagePayload{
		ID:        fmt.Sprintf("msg-%d", rand.Int63()),
		Timestamp: time.Now(),
		Size:      cfg.MessageSize,
		Data:      generateRandomData(cfg.MessageSize),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	start := time.Now()

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &cfg.QueueURL,
		MessageBody: aws.String(string(data)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"TestID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(payload.ID),
			},
			"Size": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(fmt.Sprintf("%d", cfg.MessageSize)),
			},
		},
	})

	latency := time.Since(start)

	if err == nil {
		stats.messagesSent.Add(1)
		latencyMs := latency.Milliseconds()
		stats.totalLatency.Add(latencyMs)

		// Update max latency if higher
		for {
			current := stats.maxLatency.Load()
			if latencyMs <= current {
				break
			}
			if stats.maxLatency.CompareAndSwap(current, latencyMs) {
				break
			}
		}
	}

	return err
}

func reportStats(ctx context.Context, stats *Stats) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastMessageCount int64
	var lastErrorCount int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentMessages := stats.messagesSent.Load()
			currentErrors := stats.messagesError.Load()

			// Calculate rates
			messageRate := float64(currentMessages-lastMessageCount) / 5.0
			errorRate := float64(currentErrors-lastErrorCount) / 5.0

			// Calculate average latency
			if currentMessages > lastMessageCount {
				avgLatency := time.Duration(stats.totalLatency.Load()/currentMessages) * time.Millisecond
				maxLatency := time.Duration(stats.maxLatency.Load()) * time.Millisecond

				log.Printf("Rate: %.1f msg/sec, Errors: %.1f err/sec, Avg latency: %v, Max latency: %v",
					messageRate, errorRate, avgLatency, maxLatency)
			}

			lastMessageCount = currentMessages
			lastErrorCount = currentErrors
		}
	}
}

func generateRandomData(size int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, size)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func handleShutdown(cancel context.CancelFunc, done chan struct{}) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("Received shutdown signal, stopping gracefully...")
		cancel()
	case <-done:
		return
	}
}

func parseFlags() Config {
	var cfg Config

	flag.StringVar(&cfg.QueueURL, "queue-url", "", "SQS Queue URL")
	flag.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "Test duration")
	flag.IntVar(&cfg.BaseRate, "base-rate", 10, "Base messages per second")
	flag.IntVar(&cfg.BurstRate, "burst-rate", 100, "Burst messages per second")
	flag.DurationVar(&cfg.BurstDuration, "burst-duration", 30*time.Second, "How long each burst lasts")
	flag.DurationVar(&cfg.BurstInterval, "burst-interval", 5*time.Minute, "Time between bursts")
	flag.IntVar(&cfg.MessageSize, "message-size", 1024, "Message size in bytes")
	flag.IntVar(&cfg.Concurrent, "concurrent", 5, "Number of concurrent senders")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 5*time.Second, "Timeout for individual requests")

	flag.Parse()

	if cfg.QueueURL == "" {
		log.Fatal("queue-url is required")
	}

	return cfg
}
