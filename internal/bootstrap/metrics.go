package bootstrap

import (
	"fmt"
	"sync/atomic"
	"time"
)

// metrics tracks per-phase timing aggregates used by the stats logger to
// answer "where is the bootstrap actually spending its time?" — useful when
// throughput is below what the rate limit would allow.
//
// All fields are atomic so workers can update without coordinating. Means
// are derived from sum/count at log time.
type metrics struct {
	profileCount, profileNs                 atomic.Int64
	followPageCount, followPageNs           atomic.Int64
	blockPageCount, blockPageNs             atomic.Int64
	didCount, didNs                         atomic.Int64
	chunkSendCount, chunkSendBlockedNs      atomic.Int64
	writerCount, writerNs                   atomic.Int64
}

func (m *metrics) observeProfile(d time.Duration) {
	m.profileCount.Add(1)
	m.profileNs.Add(int64(d))
}
func (m *metrics) observeFollowPage(d time.Duration) {
	m.followPageCount.Add(1)
	m.followPageNs.Add(int64(d))
}
func (m *metrics) observeBlockPage(d time.Duration) {
	m.blockPageCount.Add(1)
	m.blockPageNs.Add(int64(d))
}
func (m *metrics) observeDID(d time.Duration) {
	m.didCount.Add(1)
	m.didNs.Add(int64(d))
}
func (m *metrics) observeChunkSendBlocked(d time.Duration) {
	m.chunkSendCount.Add(1)
	m.chunkSendBlockedNs.Add(int64(d))
}
func (m *metrics) observeWriter(d time.Duration) {
	m.writerCount.Add(1)
	m.writerNs.Add(int64(d))
}

// snapshot returns a one-line summary of mean per-phase timings. Empty
// counters are reported as "—" so the line stays concise during warm-up.
func (m *metrics) snapshot() string {
	mean := func(sum, count int64) string {
		if count == 0 {
			return "—"
		}
		return time.Duration(sum / count).String()
	}
	return fmt.Sprintf(
		"profile=%s/n=%d follow_page=%s/n=%d block_page=%s/n=%d did_total=%s/n=%d chunk_send_block=%s writer_tx=%s/n=%d",
		mean(m.profileNs.Load(), m.profileCount.Load()), m.profileCount.Load(),
		mean(m.followPageNs.Load(), m.followPageCount.Load()), m.followPageCount.Load(),
		mean(m.blockPageNs.Load(), m.blockPageCount.Load()), m.blockPageCount.Load(),
		mean(m.didNs.Load(), m.didCount.Load()), m.didCount.Load(),
		mean(m.chunkSendBlockedNs.Load(), m.chunkSendCount.Load()),
		mean(m.writerNs.Load(), m.writerCount.Load()), m.writerCount.Load(),
	)
}
