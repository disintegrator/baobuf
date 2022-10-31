package baobuf

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	ErrOverflow = errors.New("insert causes unwanted overflow")
)

// Policy defines the criteria that control the buffer's size and when flushing
// should be triggered.
type Policy struct {
	// The maximum size that the buffer can reach before it is flushed. Attempting
	// to add more data to the buffer after it reaches capacity will result in an
	// error. The default value for this option will create a buffer that is not
	// bounded by size.
	Capacity int

	// The frequency with which to flush a buffer regardless of how much data it
	// has. The default value for this option will disable periodic flushing.
	Period time.Duration
}

// Buffer is concurrent data structure that can hold a collection of items with
// some policy that sets bounds on its capacity and how it is flushed.
type Buffer[T any] struct {
	m sync.Mutex

	data   []T
	policy *Policy

	flushC chan struct{}
}

// New creates a bounded buffer using a desired policy that determines when it
// should be flushed.
func New[T any](ctx context.Context, policy *Policy) *Buffer[T] {
	buf := &Buffer[T]{
		data:   make([]T, 0, policy.Capacity),
		policy: policy,

		flushC: make(chan struct{}, 1),
	}

	if policy.Period > 0 {
		go buf.timerLoop(ctx, buf.flushC, policy.Period)
	}

	return buf
}

func (b *Buffer[T]) timerLoop(rootCtx context.Context, flushC chan struct{}, period time.Duration) {
	loop := func() error {
		ctx, cancel := context.WithTimeout(rootCtx, period)
		defer cancel()

		<-ctx.Done()

		select {
		case flushC <- struct{}{}:
		default:
		}

		return ctx.Err()
	}

	for {
		if err := loop(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return
		}
	}
}

var _ io.Writer = &Buffer[byte]{}

// Writes inserts elements into the buffer and reports back the number that was
// inserted or if there was an overflow error because the buffer is at capacity.
// If the buffer's capacity policy is set to 0 then no overflow error is
// reported and instead the buffer will continue to grow with each write
// until it is flushed.
func (b *Buffer[T]) Write(elements []T) (int, error) {
	b.m.Lock()
	defer b.m.Unlock()

	n := len(elements)
	inserts := elements

	if b.policy.Capacity != 0 {
		headroom := b.policy.Capacity - len(b.data)

		if headroom <= 0 {
			return 0, ErrOverflow
		}

		if headroom < n {
			n = headroom
		}

		inserts = elements[:n]
	}

	b.data = append(b.data, inserts...)
	if b.policy.Capacity > 0 && len(b.data) >= b.policy.Capacity {
		select {
		case b.flushC <- struct{}{}:
		default:
		}
	}

	return n, nil
}

// Flush returns the contents of the buffer when a policy condition is met and
// resets its state.
func (b *Buffer[T]) Flush(ctx context.Context) []T {
	for {
		select {
		case <-ctx.Done():
		case <-b.flushC:
		}

		if data := b.flush(); len(data) != 0 {
			return data
		}
	}
}

func (b *Buffer[T]) flush() []T {
	var empty []T

	b.m.Lock()
	defer b.m.Unlock()

	data := b.data
	if len(data) == 0 {
		return empty
	}

	b.data = make([]T, 0, b.policy.Capacity)

	return data
}
