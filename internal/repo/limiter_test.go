package repo

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHostLimiterBaseline(t *testing.T) {
	// 10 RPS, burst 1: a tight bound where each Wait should serialize.
	l := NewHostLimiter(10, 1)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := l.Wait(ctx, "example.com"); err != nil {
			t.Fatal(err)
		}
	}
	// 5 acquisitions at 10 RPS with burst 1 takes ~400ms (first is free,
	// the next 4 wait 100ms each).
	elapsed := time.Since(start)
	if elapsed < 350*time.Millisecond {
		t.Errorf("limiter too permissive: 5 waits in %v at 10rps/burst1", elapsed)
	}
	if elapsed > 800*time.Millisecond {
		t.Errorf("limiter too strict: %v", elapsed)
	}
}

func TestHostLimiterIsolatesHosts(t *testing.T) {
	// A request on host A shouldn't drain host B's bucket.
	l := NewHostLimiter(10, 1)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := l.Wait(ctx, "a.example.com"); err != nil {
			t.Fatal(err)
		}
	}
	// Fresh host: first acquisition is essentially free.
	start := time.Now()
	if err := l.Wait(ctx, "b.example.com"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("first call on fresh host blocked %v", d)
	}
}

func TestHostLimiterPenalizeTightens(t *testing.T) {
	l := NewHostLimiter(100, 100) // permissive baseline
	ctx := context.Background()
	if err := l.Wait(ctx, "example.com"); err != nil {
		t.Fatal(err)
	}
	l.Penalize("example.com") // halves to 50 RPS
	// Drain the burst, then time the next 5 waits.
	for i := 0; i < 100; i++ {
		_ = l.Wait(ctx, "example.com")
	}
	start := time.Now()
	for i := 0; i < 5; i++ {
		_ = l.Wait(ctx, "example.com")
	}
	// 5 ticks at 50 RPS ≈ 100ms minimum.
	if d := time.Since(start); d < 80*time.Millisecond {
		t.Errorf("penalty did not slow throughput: %v", d)
	}
}

func TestHostLimiterNilSafe(t *testing.T) {
	// Methods on nil should no-op for tests that don't construct one.
	var l *HostLimiter
	if err := l.Wait(context.Background(), "x"); err != nil {
		t.Errorf("nil Wait err: %v", err)
	}
	l.Penalize("x")
}

func TestHostLimiterRespectsContext(t *testing.T) {
	l := NewHostLimiter(0.001, 1) // effectively blocked after burst
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_ = l.Wait(ctx, "example.com") // first one is free (burst)
	if err := l.Wait(ctx, "example.com"); err == nil {
		t.Errorf("expected ctx-deadline error, got nil")
	}
}

// Ensure concurrent Waits don't race on the map.
func TestHostLimiterConcurrentSafe(t *testing.T) {
	l := NewHostLimiter(1000, 100)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			host := "host"
			if i%2 == 0 {
				host = "other-host"
			}
			for j := 0; j < 10; j++ {
				_ = l.Wait(ctx, host)
			}
		}(i)
	}
	wg.Wait()
}
