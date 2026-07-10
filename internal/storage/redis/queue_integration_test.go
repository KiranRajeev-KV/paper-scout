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
	queue := NewQueue(client, QueueOptions{
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
