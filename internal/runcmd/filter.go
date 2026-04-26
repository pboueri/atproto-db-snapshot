package runcmd

import (
	"strings"

	"github.com/pboueri/atproto-db-snapshot/internal/atrecord"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// filter encapsulates the language/label filtering rules from the spec.
//
// Engagement records (follow, like, repost, block) carry no language or label
// metadata of their own and are therefore never filtered — only post records
// (and their derived post_media sidecars) are subject to filtering.
type filter struct {
	languages     map[string]struct{} // empty = no filter
	includeLabels map[string]struct{} // empty = no allow-list
	excludeLabels map[string]struct{} // empty = no deny-list
}

func newFilter(languages, includeLabels, excludeLabels []string) filter {
	return filter{
		languages:     toSet(languages),
		includeLabels: toSet(includeLabels),
		excludeLabels: toSet(excludeLabels),
	}
}

func toSet(s []string) map[string]struct{} {
	if len(s) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(s))
	for _, v := range s {
		out[v] = struct{}{}
	}
	return out
}

// keepPost decides whether a post survives the filter. Returns true if any of
// the post's languages is in the allow-list (or there is no language filter)
// AND the post has no excluded labels AND, if includeLabels is set, the post
// has at least one included label.
func (f filter) keepPost(p model.Post) bool {
	if len(f.languages) > 0 {
		anyMatch := false
		for _, l := range atrecord.LangsFromPost(p) {
			if _, ok := f.languages[strings.ToLower(strings.TrimSpace(l))]; ok {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false
		}
	}
	if len(f.excludeLabels) > 0 {
		for _, lbl := range splitCSV(p.Labels) {
			if _, ok := f.excludeLabels[lbl]; ok {
				return false
			}
		}
	}
	if len(f.includeLabels) > 0 {
		any := false
		for _, lbl := range splitCSV(p.Labels) {
			if _, ok := f.includeLabels[lbl]; ok {
				any = true
				break
			}
		}
		if !any {
			return false
		}
	}
	return true
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
