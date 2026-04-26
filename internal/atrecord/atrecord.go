// Package atrecord decodes the JSON record payloads emitted by Constellation
// (during bootstrap) and Jetstream (during run) into typed model values.
//
// Record payloads share a small core schema across collections — $type,
// createdAt — but each collection adds its own fields. We unmarshal into
// per-collection raw structs and translate to model types so the rest of the
// pipeline stays JSON-free.
package atrecord

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/intern"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// rawProfile is the subset of app.bsky.actor.profile fields we keep.
type rawProfile struct {
	DisplayName string         `json:"displayName"`
	Description string         `json:"description"`
	Avatar      *blobRef       `json:"avatar"`
	Banner      *blobRef       `json:"banner"`
	CreatedAt   *flexTime      `json:"createdAt"`
}

type rawFollow struct {
	Subject   string    `json:"subject"`
	CreatedAt *flexTime `json:"createdAt"`
}

type rawBlock struct {
	Subject   string    `json:"subject"`
	CreatedAt *flexTime `json:"createdAt"`
}

type rawPost struct {
	Text      string         `json:"text"`
	Langs     []string       `json:"langs"`
	Labels    *postLabelsRef `json:"labels"`
	Reply     *replyRef      `json:"reply"`
	Embed     json.RawMessage `json:"embed"`
	CreatedAt *flexTime      `json:"createdAt"`
}

type rawLike struct {
	Subject   subjectRef `json:"subject"`
	CreatedAt *flexTime  `json:"createdAt"`
}

type rawRepost struct {
	Subject   subjectRef `json:"subject"`
	CreatedAt *flexTime  `json:"createdAt"`
}

type subjectRef struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
}

type replyRef struct {
	Parent subjectRef `json:"parent"`
	Root   subjectRef `json:"root"`
}

type postLabelsRef struct {
	Type   string `json:"$type"`
	Values []struct {
		Val string `json:"val"`
	} `json:"values"`
}

type blobRef struct {
	Ref struct {
		Link string `json:"$link"`
	} `json:"ref"`
	MimeType string `json:"mimeType"`
}

// flexTime accepts the variety of timestamp formats records use in practice:
// strict RFC3339, RFC3339 without timezone, and dates with milliseconds. If
// none parse it leaves the zero time and lets the caller fill in indexedAt.
type flexTime struct {
	t time.Time
}

var timeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05",
}

func (f *flexTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return nil // tolerate non-string timestamps; t stays zero
	}
	for _, fmt := range timeFormats {
		if t, err := time.Parse(fmt, s); err == nil {
			f.t = t.UTC()
			return nil
		}
	}
	return nil
}

func (f *flexTime) Time() time.Time {
	if f == nil {
		return time.Time{}
	}
	return f.t
}

// DecodeProfile turns a raw constellation record value into a model.Profile.
// did is the repo DID; indexedAt is the wall-clock observation time.
func DecodeProfile(value json.RawMessage, did string, indexedAt time.Time, source string) (model.Profile, error) {
	var raw rawProfile
	if err := json.Unmarshal(value, &raw); err != nil {
		return model.Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	return model.Profile{
		DID:         did,
		DIDID:       intern.DIDID(did),
		DisplayName: raw.DisplayName,
		Description: raw.Description,
		AvatarCID:   blobLink(raw.Avatar),
		BannerCID:   blobLink(raw.Banner),
		CreatedAt:   raw.CreatedAt.Time(),
		IndexedAt:   indexedAt.UTC(),
		Op:          model.OpCreate,
		Source:      source,
	}, nil
}

// DecodeFollow expects rkey because the at-uri carries it; we keep the rkey
// for downstream tombstone reconciliation.
func DecodeFollow(value json.RawMessage, srcDID, rkey string, indexedAt time.Time, source string) (model.Follow, error) {
	var raw rawFollow
	if err := json.Unmarshal(value, &raw); err != nil {
		return model.Follow{}, fmt.Errorf("decode follow: %w", err)
	}
	return model.Follow{
		SrcDID:    srcDID,
		SrcDIDID:  intern.DIDID(srcDID),
		DstDID:    raw.Subject,
		DstDIDID:  intern.DIDID(raw.Subject),
		RKey:      rkey,
		CreatedAt: raw.CreatedAt.Time(),
		IndexedAt: indexedAt.UTC(),
		Op:        model.OpCreate,
		Source:    source,
	}, nil
}

func DecodeBlock(value json.RawMessage, srcDID, rkey string, indexedAt time.Time, source string) (model.Block, error) {
	var raw rawBlock
	if err := json.Unmarshal(value, &raw); err != nil {
		return model.Block{}, fmt.Errorf("decode block: %w", err)
	}
	return model.Block{
		SrcDID:    srcDID,
		SrcDIDID:  intern.DIDID(srcDID),
		DstDID:    raw.Subject,
		DstDIDID:  intern.DIDID(raw.Subject),
		RKey:      rkey,
		CreatedAt: raw.CreatedAt.Time(),
		IndexedAt: indexedAt.UTC(),
		Op:        model.OpCreate,
		Source:    source,
	}, nil
}

// DecodePost decodes a post record and returns the post plus any media
// sidecars derived from the embed. cid is the post's commit cid (optional).
func DecodePost(value json.RawMessage, did, rkey, cid string, indexedAt time.Time, source string) (model.Post, []model.PostMedia, error) {
	var raw rawPost
	if err := json.Unmarshal(value, &raw); err != nil {
		return model.Post{}, nil, fmt.Errorf("decode post: %w", err)
	}
	uri := fmt.Sprintf("at://%s/%s/%s", did, model.CollectionPost, rkey)
	post := model.Post{
		URI:       uri,
		URIID:     intern.URIID(uri),
		DID:       did,
		DIDID:     intern.DIDID(did),
		RKey:      rkey,
		CID:       cid,
		Text:      raw.Text,
		Langs:     strings.Join(raw.Langs, ","),
		Labels:    joinLabels(raw.Labels),
		CreatedAt: raw.CreatedAt.Time(),
		IndexedAt: indexedAt.UTC(),
		Op:        model.OpCreate,
		Source:    source,
	}
	if raw.Reply != nil {
		post.ReplyParentURI = raw.Reply.Parent.URI
		post.ReplyParentID = intern.URIID(raw.Reply.Parent.URI)
		post.ReplyRootURI = raw.Reply.Root.URI
		post.ReplyRootID = intern.URIID(raw.Reply.Root.URI)
	}
	media, err := extractMedia(raw.Embed, post)
	if err != nil {
		return post, nil, fmt.Errorf("decode post embed: %w", err)
	}
	if len(media) > 0 {
		post.HasMedia = true
	}
	// Quote (record embed) URI if the embed is a record union variant.
	post.QuoteParentURI, post.QuoteParentID = extractQuote(raw.Embed)
	return post, media, nil
}

func DecodeLike(value json.RawMessage, actorDID, rkey string, indexedAt time.Time, source string) (model.Like, error) {
	var raw rawLike
	if err := json.Unmarshal(value, &raw); err != nil {
		return model.Like{}, fmt.Errorf("decode like: %w", err)
	}
	return model.Like{
		ActorDID:   actorDID,
		ActorDIDID: intern.DIDID(actorDID),
		SubjectURI: raw.Subject.URI,
		SubjectID:  intern.URIID(raw.Subject.URI),
		RKey:       rkey,
		CreatedAt:  raw.CreatedAt.Time(),
		IndexedAt:  indexedAt.UTC(),
		Op:         model.OpCreate,
		Source:     source,
	}, nil
}

func DecodeRepost(value json.RawMessage, actorDID, rkey string, indexedAt time.Time, source string) (model.Repost, error) {
	var raw rawRepost
	if err := json.Unmarshal(value, &raw); err != nil {
		return model.Repost{}, fmt.Errorf("decode repost: %w", err)
	}
	return model.Repost{
		ActorDID:   actorDID,
		ActorDIDID: intern.DIDID(actorDID),
		SubjectURI: raw.Subject.URI,
		SubjectID:  intern.URIID(raw.Subject.URI),
		RKey:       rkey,
		CreatedAt:  raw.CreatedAt.Time(),
		IndexedAt:  indexedAt.UTC(),
		Op:         model.OpCreate,
		Source:     source,
	}, nil
}

// LangsFromPost returns the parsed list form of a Post.Langs.
func LangsFromPost(p model.Post) []string {
	if p.Langs == "" {
		return nil
	}
	return strings.Split(p.Langs, ",")
}

func blobLink(b *blobRef) string {
	if b == nil {
		return ""
	}
	return b.Ref.Link
}

func joinLabels(l *postLabelsRef) string {
	if l == nil {
		return ""
	}
	out := make([]string, 0, len(l.Values))
	for _, v := range l.Values {
		out = append(out, v.Val)
	}
	return strings.Join(out, ",")
}

// embedEnvelope decodes just the $type discriminator so we can branch.
type embedEnvelope struct {
	Type string `json:"$type"`
	// Variant fields — at most one will be non-nil after decode.
	Images []embedImage     `json:"images"`
	Video  *embedVideo      `json:"video"`
	External *embedExternal `json:"external"`
	Record *embedRecordRef  `json:"record"`
	Media  *embedRecordWithMedia `json:"media"`
}

type embedImage struct {
	Image    blobRef `json:"image"`
	Alt      string  `json:"alt"`
}

type embedVideo struct {
	Video blobRef `json:"video"`
}

type embedExternal struct {
	URI         string `json:"uri"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type embedRecordRef struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
}

type embedRecordWithMedia struct {
	Type   string         `json:"$type"`
	Images []embedImage   `json:"images"`
	Video  *embedVideo    `json:"video"`
	External *embedExternal `json:"external"`
}

func extractMedia(embed json.RawMessage, post model.Post) ([]model.PostMedia, error) {
	if len(embed) == 0 {
		return nil, nil
	}
	var env embedEnvelope
	if err := json.Unmarshal(embed, &env); err != nil {
		return nil, err
	}
	var out []model.PostMedia
	add := func(idx int32, mediaType, url, cid string) {
		out = append(out, model.PostMedia{
			PostURI:   post.URI,
			PostURIID: post.URIID,
			DID:       post.DID,
			DIDID:     post.DIDID,
			Index:     idx,
			MediaType: mediaType,
			URL:       url,
			BlobCID:   cid,
			CreatedAt: post.CreatedAt,
			IndexedAt: post.IndexedAt,
		})
	}
	switch env.Type {
	case "app.bsky.embed.images":
		for i, im := range env.Images {
			add(int32(i), "image", "", im.Image.Ref.Link)
		}
	case "app.bsky.embed.video":
		if env.Video != nil {
			add(0, "video", "", env.Video.Video.Ref.Link)
		}
	case "app.bsky.embed.external":
		if env.External != nil {
			add(0, "external", env.External.URI, "")
		}
	case "app.bsky.embed.record":
		// Record-only embed (a quote): no media to record here, the quote
		// reference is captured by extractQuote.
	case "app.bsky.embed.recordWithMedia":
		if env.Media != nil {
			for i, im := range env.Media.Images {
				add(int32(i), "image", "", im.Image.Ref.Link)
			}
			if env.Media.Video != nil {
				add(int32(len(env.Media.Images)), "video", "", env.Media.Video.Video.Ref.Link)
			}
			if env.Media.External != nil {
				add(int32(len(env.Media.Images)), "external", env.Media.External.URI, "")
			}
		}
	}
	return out, nil
}

func extractQuote(embed json.RawMessage) (string, int64) {
	if len(embed) == 0 {
		return "", 0
	}
	var env embedEnvelope
	if err := json.Unmarshal(embed, &env); err != nil {
		return "", 0
	}
	switch env.Type {
	case "app.bsky.embed.record":
		if env.Record != nil {
			return env.Record.URI, intern.URIID(env.Record.URI)
		}
	case "app.bsky.embed.recordWithMedia":
		if env.Record != nil {
			return env.Record.URI, intern.URIID(env.Record.URI)
		}
	}
	return "", 0
}
