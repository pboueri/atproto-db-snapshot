package duckdbstore

import "time"

// FollowRow is a follows_current row ready to append.
type FollowRow struct {
	SrcID     int64
	DstID     int64
	Rkey      string
	CreatedAt time.Time
}

// BlockRow is a blocks_current row ready to append.
type BlockRow struct {
	SrcID     int64
	DstID     int64
	Rkey      string
	CreatedAt time.Time
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilIfZero(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
