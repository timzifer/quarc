package service

import (
	"context"
	"sync"
	"sync/atomic"
)

func runWorkerPool[T any](ctx context.Context, slots int, items []T, fn func(context.Context, T) int) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if slots <= 1 || len(items) <= 1 {
		total := 0
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return total, true
			}
			total += fn(ctx, item)
		}
		return total, false
	}

	tasks := make(chan T)
	results := make(chan int)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for item := range tasks {
			if ctx.Err() != nil {
				continue
			}
			results <- fn(ctx, item)
		}
	}

	for i := 0; i < slots; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var aborted atomic.Bool
	go func() {
		for _, item := range items {
			if ctx.Err() != nil {
				aborted.Store(true)
				break
			}
			tasks <- item
		}
		close(tasks)
	}()

	total := 0
	for result := range results {
		total += result
	}
	return total, aborted.Load()
}
