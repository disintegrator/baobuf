package baobuf

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBuffer(t *testing.T) {
	p := &Policy{Capacity: 10}
	buf := New[struct{}](context.Background(), p)

	els := make([]struct{}, p.Capacity)

	n, err := buf.Write(els)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != len(els) {
		t.Fatalf("expected %d elements to be written to buffer but got %d", p.Capacity, n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	out := buf.Flush(ctx)
	if len(out) != len(els) {
		t.Fatalf("did not get expected number of elements after flush: got: %d - wanted: %d", len(out), len(els))
	}
}

func TestBuffer_Concurrent(t *testing.T) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &Policy{Capacity: 10}
	results := make(chan []struct{}, 1)
	buf := New[struct{}](context.Background(), p)

	wg.Add(1)
	for i := 0; i < p.Capacity; i++ {
		go func() {
			if res := buf.Flush(ctx); len(res) > 0 {
				defer wg.Done()
				results <- res
			}
		}()
	}

	els := make([]struct{}, p.Capacity)
	n, err := buf.Write(els)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != len(els) {
		t.Fatalf("expected %d elements to be written to buffer but got %d", p.Capacity, n)
	}

	wg.Wait()
	close(results)

	out := 0

	for v := range results {
		out += len(v)
	}

	if out != len(els) {
		t.Fatalf("did not get expected number of elements after flush: got: %d - wanted: %d", out, len(els))
	}
}

func TestBuffer_PeriodPolicy(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cap := 5
	p := &Policy{Capacity: 10, Period: 100 * time.Millisecond}
	results := make(chan []struct{}, 1)
	buf := New[struct{}](context.Background(), p)

	wg.Add(1)
	for i := 0; i < cap; i++ {
		go func() {
			if res := buf.Flush(ctx); len(res) > 0 {
				defer wg.Done()
				results <- res
			}
		}()
	}

	els := make([]struct{}, cap)
	n, err := buf.Write(els)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != len(els) {
		t.Fatalf("expected %d elements to be written to buffer but got %d", cap, n)
	}

	wg.Wait()
	close(results)

	out := 0

	for v := range results {
		out += len(v)
	}

	if out != len(els) {
		t.Fatalf("did not get expected number of elements after flush: got: %d - wanted: %d", out, len(els))
	}
}

func TestBuffer_PartialWrite(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cap := 20
	p := &Policy{Capacity: 10, Period: 100 * time.Millisecond}
	results := make(chan []struct{}, 1)
	buf := New[struct{}](context.Background(), p)

	wg.Add(1)
	for i := 0; i < cap; i++ {
		go func() {
			if res := buf.Flush(ctx); len(res) > 0 {
				defer wg.Done()
				results <- res
			}
		}()
	}

	els := make([]struct{}, cap)
	n, err := buf.Write(els)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != p.Capacity {
		t.Fatalf("expected %d elements to be written to buffer but got %d", p.Capacity, n)
	}

	wg.Wait()
	close(results)

	out := 0

	for v := range results {
		out += len(v)
	}

	if out != p.Capacity {
		t.Fatalf("did not get expected number of elements after flush: got: %d - wanted: %d", out, p.Capacity)
	}
}

func TestBuffer_Overflow(t *testing.T) {
	cap := 20
	p := &Policy{Capacity: 10, Period: 100 * time.Millisecond}
	buf := New[struct{}](context.Background(), p)

	els := make([]struct{}, cap)
	n, err := buf.Write(els)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != p.Capacity {
		t.Fatalf("expected %d elements to be written to buffer but got %d", p.Capacity, n)
	}

	n, err = buf.Write(els[n:])
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected overflow error but got: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected %d elements to be written to buffer but got %d", 0, n)
	}
}
