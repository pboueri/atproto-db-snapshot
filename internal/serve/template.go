package serve

import (
	"fmt"
	"html/template"
	"io/fs"
	"os"
	"sort"
	"time"
)

// osStatFn is a small wrapper so server.go can ask "size of file" without
// pulling in the rest of os.FileInfo.
func osStatFn(path string) (fs.FileInfo, error) { return os.Stat(path) }

var tmplFuncs = template.FuncMap{
	"humanBytes":    humanBytes,
	"humanInt":      humanInt,
	"humanDuration": humanDuration,
	"humanLag":      humanLag,
	"humanRate":     humanRate,
	"timeUTC":       timeUTC,
	"timeAgo":       timeAgo,
	"orDash":        orDash,
	"sortedKeys":    sortedKeys,
	"sortedKeysAny": sortedKeysAny,
	"sortedKeysInt": sortedKeysInt,
	"int64Map":      int64MapAccess,
}

// orDash returns "—" for the empty/zero value of common types so the
// template doesn't sprout `if … else` chains everywhere.
func orDash(v any) string {
	if v == nil {
		return "—"
	}
	switch t := v.(type) {
	case string:
		if t == "" {
			return "—"
		}
		return t
	case time.Time:
		if t.IsZero() {
			return "—"
		}
		return t.UTC().Format("2006-01-02 15:04 UTC")
	case int:
		if t == 0 {
			return "—"
		}
		return humanInt(int64(t))
	case int64:
		if t == 0 {
			return "—"
		}
		return humanInt(t)
	case float64:
		if t == 0 {
			return "—"
		}
		return fmt.Sprintf("%g", t)
	case uint64:
		if t == 0 {
			return "—"
		}
		return humanInt(int64(t))
	}
	return fmt.Sprintf("%v", v)
}

func humanBytes(n any) string {
	v, ok := asInt64(n)
	if !ok || v <= 0 {
		return "—"
	}
	const unit = 1024.0
	f := float64(v)
	suffixes := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	i := 0
	for f >= unit && i < len(suffixes)-1 {
		f /= unit
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d %s", v, suffixes[i])
	}
	return fmt.Sprintf("%.2f %s", f, suffixes[i])
}

func humanInt(n int64) string {
	if n == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", n)
	// thousands separator with spaces (matches §10 examples like "35 102 443")
	out := []byte{}
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ' ')
		}
		out = append(out, c)
	}
	return string(out)
}

func humanDuration(seconds any) string {
	f, ok := asFloat64(seconds)
	if !ok || f <= 0 {
		return "—"
	}
	d := time.Duration(f * float64(time.Second))
	if d < time.Minute {
		return fmt.Sprintf("%.0f s", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%d min", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}

func humanLag(seconds any) string {
	f, ok := asFloat64(seconds)
	if !ok {
		return "—"
	}
	if f < 0 {
		f = -f
	}
	d := time.Duration(f * float64(time.Second))
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func humanRate(perSec any) string {
	f, ok := asFloat64(perSec)
	if !ok {
		return "—"
	}
	if f == 0 {
		return "0"
	}
	if f < 10 {
		return fmt.Sprintf("%.2f/s", f)
	}
	return fmt.Sprintf("%s/s", humanInt(int64(f)))
}

func timeUTC(v any) string {
	t, ok := v.(time.Time)
	if !ok || t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func timeAgo(v any) string {
	t, ok := v.(time.Time)
	if !ok || t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	if d < time.Minute {
		return fmt.Sprintf("%d s ago", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%d min ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%d h ago", int(d.Hours()))
	}
	return fmt.Sprintf("%d d ago", int(d.Hours()/24))
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysInt(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysAny(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func int64MapAccess(m map[string]int64, k string) int64 { return m[k] }

func asInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case float64:
		return int64(t), true
	case float32:
		return int64(t), true
	}
	return 0, false
}

func asFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case float64:
		return t, true
	case float32:
		return float64(t), true
	}
	return 0, false
}

