package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Protects redis queue final failure is terminal.
func TestRedisQueueFinalFailureIsTerminal(t *testing.T) {
	address := os.Getenv("PAPER_SCOUT_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("PAPER_SCOUT_TEST_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{
		Addr:     address,
		Password: os.Getenv("PAPER_SCOUT_TEST_REDIS_PASSWORD"),
		DB:       0,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}
	defer client.Close()

	name := uuid.NewString()
	queue := NewQueue(client, client, QueueOptions{
		Stream: "test:jobs:" + name,
		Group:  "test:workers:" + name,
	})
	if err := queue.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { _ = client.Del(ctx, queue.stream, JobQueueDeadLetter).Err() })

	if err := queue.Enqueue(ctx, Job{
		ID:       fmt.Sprintf("job-%s", name),
		Type:     JobTypePaperAnalysis,
		MaxRetry: 3,
		Retries:  2,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, err := queue.Dequeue(ctx, "test-consumer", time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if job == nil {
		t.Fatal("dequeue returned no job")
	}
	result, err := queue.Fail(ctx, *job, "permanent failure")
	if err != nil {
		t.Fatalf("fail job: %v", err)
	}
	if !result.Terminal || result.Requeued {
		t.Fatalf("failure result = %+v, want terminal non-requeued", result)
	}
}

// Protects control-plane operations from workers blocked in XREADGROUP.
func TestRedisQueueBlockingConsumerDoesNotStarveControlClient(t *testing.T) {
	address := os.Getenv("PAPER_SCOUT_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("PAPER_SCOUT_TEST_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	controlClient := goredis.NewClient(&goredis.Options{
		Addr: address, Password: os.Getenv("PAPER_SCOUT_TEST_REDIS_PASSWORD"), DB: 0, PoolSize: 1,
	})
	workerClient := goredis.NewClient(&goredis.Options{
		Addr: address, Password: os.Getenv("PAPER_SCOUT_TEST_REDIS_PASSWORD"), DB: 0, PoolSize: 1,
	})
	defer controlClient.Close()
	defer workerClient.Close()
	if err := controlClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	name := uuid.NewString()
	queue := NewQueue(controlClient, workerClient, QueueOptions{
		Stream: "test:jobs:" + name,
		Group:  "test:workers:" + name,
	})
	if err := queue.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { _ = controlClient.Del(ctx, queue.stream).Err() })

	done := make(chan error, 1)
	go func() {
		_, err := queue.Dequeue(ctx, "blocking-consumer", 2*time.Second)
		done <- err
	}()

	deadline := time.Now().Add(time.Second)
	for {
		stats := workerClient.PoolStats()
		if stats.TotalConns == 1 && stats.IdleConns == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker client did not enter its blocking stream read")
		}
		time.Sleep(10 * time.Millisecond)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := controlClient.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("control ping while worker is blocked: %v", err)
	}
	if err := controlClient.Set(pingCtx, "test:control:"+name, "ok", 0).Err(); err != nil {
		t.Fatalf("control state write while worker is blocked: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("blocking dequeue: %v", err)
	}
}
