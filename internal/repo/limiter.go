package repo

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// HostLimiter is a per-host token bucket that gates all in-flight requests
// across the worker pool. We key by hostname (not full URL) because rate
// limits live at the PDS process level — every DID hosted on bsky.social
// shares the same budget, regardless of which one we're listing right now.
//
// The limiter is adaptive: when a request comes back 429, callers report
// it via Penalize, which halves the host's allowed RPS for a recovery
// window before easing back toward the configured baseline. That's far
// better than retrying-with-backoff alone, which papers over the symptom
// without slowing down the *next* request that would have been throttled.
type HostLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	baseline rate.Limit
	burst    int

	// Adaptive recovery state, also keyed by host.
	penaltyUntil map[string]time.Time
	penaltyRPS   rate.Limit
	penaltyFor   time.Duration
}

// NewHostLimiter returns a limiter where each host is initially allowed up
// to baselineRPS sustained, with bursts up to burst tokens.
func NewHostLimiter(baselineRPS float64, burst int) *HostLimiter {
	if baselineRPS <= 0 {
		baselineRPS = 5
	}
	if burst <= 0 {
		burst = int(baselineRPS)
		if burst <= 0 {
			burst = 5
		}
	}
	return &HostLimiter{
		limiters:     map[string]*rate.Limiter{},
		penaltyUntil: map[string]time.Time{},
		baseline:     rate.Limit(baselineRPS),
		burst:        burst,
		// Penalty halves throughput for 60s on a 429.
		penaltyRPS: rate.Limit(baselineRPS / 2),
		penaltyFor: 60 * time.Second,
	}
}

// Wait blocks until the host's bucket has a token (or ctx is cancelled).
// Hosts unseen before get a fresh limiter at the baseline rate.
func (h *HostLimiter) Wait(ctx context.Context, host string) error {
	if h == nil {
		return nil
	}
	l := h.forHost(host)
	return l.Wait(ctx)
}

// Penalize records that host returned a 429. We tighten the host's limit
// for penaltyFor and let the adaptive recovery loop ease it back later.
func (h *HostLimiter) Penalize(host string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	l := h.limiters[host]
	if l == nil {
		l = rate.NewLimiter(h.baseline, h.burst)
		h.limiters[host] = l
	}
	l.SetLimit(h.penaltyRPS)
	h.penaltyUntil[host] = time.Now().Add(h.penaltyFor)
}

// forHost returns the per-host limiter, creating it if absent. If the host
// is in a recovery window, the function checks whether we've passed the
// penalty horizon and restores the baseline rate.
func (h *HostLimiter) forHost(host string) *rate.Limiter {
	h.mu.Lock()
	defer h.mu.Unlock()
	l, ok := h.limiters[host]
	if !ok {
		l = rate.NewLimiter(h.baseline, h.burst)
		h.limiters[host] = l
		return l
	}
	if until, penalized := h.penaltyUntil[host]; penalized && time.Now().After(until) {
		delete(h.penaltyUntil, host)
		l.SetLimit(h.baseline)
	}
	return l
}
