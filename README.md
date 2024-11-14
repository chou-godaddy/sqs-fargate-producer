# sqs-fargate-producer

# Steady low load
go run cmd/loadtest/main.go \
  --queue-url="YOUR_QUEUE_URL" \
  --base-rate=5 \
  --duration=1h

# Heavy burst testing
go run cmd/loadtest/main.go \
  --queue-url="YOUR_QUEUE_URL" \
  --base-rate=500 \
  --burst-rate=2000 \
  --burst-duration=3m \
  --burst-interval=1m \
  --duration=2h

# High concurrency test
go run cmd/loadtest/main.go \
  --queue-url="YOUR_QUEUE_URL" \
  --base-rate=1000 \
  --concurrent=10 \
  --message-size=4096