package parse

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
)

// Records holds everything extracted from a single repo CAR.
// DIDs in *_did / subject fields are raw — interning happens downstream.
type Records struct {
	AuthorDID string

	Profile *ProfileRec

	Posts   []PostRec
	Likes   []LikeRec
	Reposts []RepostRec
	Follows []FollowRec
	Blocks  []BlockRec
}

type ProfileRec struct {
	DisplayName *string
	Description *string
	AvatarCID   *string
	CreatedAt   *time.Time
}

type PostRec struct {
	Rkey                string
	CID                 string
	CreatedAt           time.Time
	Text                string
	Lang                string // first lang only (simplification)
	ReplyRootAuthorDID  string
	ReplyRootRkey       string
	ReplyParentAuthorDID string
	ReplyParentRkey     string
	QuoteAuthorDID      string
	QuoteRkey           string
	EmbedType           string

	// Structured embed detail. Populated alongside EmbedType so a
	// downstream PostEmbedEvent can be derived without re-parsing.
	// Spec: 002_post_embeds.md.
	EmbedKind            string // see 002 §1.1 (e.g. "recordWithMedia:images")
	EmbedExternalURI     string
	EmbedExternalDomain  string
	EmbedExternalTitle   string
	EmbedImageCount      int
	EmbedImageWithAlt    int
	EmbedHasVideo        bool
	EmbedVideoHasAlt     bool
}

type LikeRec struct {
	Rkey             string
	SubjectAuthorDID string
	SubjectRkey      string
	CreatedAt        time.Time
}

type RepostRec struct {
	Rkey             string
	SubjectAuthorDID string
	SubjectRkey      string
	CreatedAt        time.Time
}

type FollowRec struct {
	Rkey      string
	TargetDID string
	CreatedAt time.Time
}

type BlockRec struct {
	Rkey      string
	TargetDID string
	CreatedAt time.Time
}

// Parse walks a CAR file and returns typed records for the six collections we care about.
func Parse(ctx context.Context, did string, carBytes []byte) (*Records, error) {
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(carBytes))
	if err != nil {
		return nil, err
	}
	out := &Records{AuthorDID: did}

	err = r.ForEach(ctx, "", func(rpath string, _ cid.Cid) error {
		slash := strings.IndexByte(rpath, '/')
		if slash < 0 {
			return nil
		}
		nsid := rpath[:slash]
		rkey := rpath[slash+1:]

		switch nsid {
		case "app.bsky.feed.post",
			"app.bsky.feed.like",
			"app.bsky.feed.repost",
			"app.bsky.graph.follow",
			"app.bsky.graph.block",
			"app.bsky.actor.profile":
		default:
			return nil
		}

		_, rec, err := r.GetRecord(ctx, rpath)
		if err != nil {
			return nil
		}

		switch v := rec.(type) {
		case *bsky.FeedPost:
			out.Posts = append(out.Posts, convertPost(rkey, v))
		case *bsky.FeedLike:
			if v.Subject == nil {
				return nil
			}
			author, rk := splitATURI(v.Subject.Uri)
			out.Likes = append(out.Likes, LikeRec{
				Rkey:             rkey,
				SubjectAuthorDID: author,
				SubjectRkey:      rk,
				CreatedAt:        parseAtTime(v.CreatedAt),
			})
		case *bsky.FeedRepost:
			if v.Subject == nil {
				return nil
			}
			author, rk := splitATURI(v.Subject.Uri)
			out.Reposts = append(out.Reposts, RepostRec{
				Rkey:             rkey,
				SubjectAuthorDID: author,
				SubjectRkey:      rk,
				CreatedAt:        parseAtTime(v.CreatedAt),
			})
		case *bsky.GraphFollow:
			out.Follows = append(out.Follows, FollowRec{
				Rkey:      rkey,
				TargetDID: v.Subject,
				CreatedAt: parseAtTime(v.CreatedAt),
			})
		case *bsky.GraphBlock:
			out.Blocks = append(out.Blocks, BlockRec{
				Rkey:      rkey,
				TargetDID: v.Subject,
				CreatedAt: parseAtTime(v.CreatedAt),
			})
		case *bsky.ActorProfile:
			p := &ProfileRec{
				DisplayName: v.DisplayName,
				Description: v.Description,
			}
			if v.Avatar != nil {
				s := cid.Cid(v.Avatar.Ref).String()
				p.AvatarCID = &s
			}
			if v.CreatedAt != nil {
				t := parseAtTime(*v.CreatedAt)
				p.CreatedAt = &t
			}
			out.Profile = p
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	return out, nil
}

func convertPost(rkey string, v *bsky.FeedPost) PostRec {
	p := PostRec{
		Rkey:      rkey,
		Text:      v.Text,
		CreatedAt: parseAtTime(v.CreatedAt),
	}
	if len(v.Langs) > 0 {
		p.Lang = v.Langs[0]
	}
	if v.Reply != nil {
		if v.Reply.Root != nil {
			p.ReplyRootAuthorDID, p.ReplyRootRkey = splitATURI(v.Reply.Root.Uri)
		}
		if v.Reply.Parent != nil {
			p.ReplyParentAuthorDID, p.ReplyParentRkey = splitATURI(v.Reply.Parent.Uri)
		}
	}
	if v.Embed != nil {
		switch {
		case v.Embed.EmbedImages != nil:
			p.EmbedType = "images"
			p.EmbedKind = "images"
			fillImages(&p, v.Embed.EmbedImages)
		case v.Embed.EmbedVideo != nil:
			p.EmbedType = "video"
			p.EmbedKind = "video"
			fillVideo(&p, v.Embed.EmbedVideo)
		case v.Embed.EmbedExternal != nil:
			p.EmbedType = "external"
			p.EmbedKind = "external"
			fillExternal(&p, v.Embed.EmbedExternal)
		case v.Embed.EmbedRecord != nil:
			p.EmbedType = "record"
			p.EmbedKind = "record"
			if v.Embed.EmbedRecord.Record != nil {
				p.QuoteAuthorDID, p.QuoteRkey = splitATURI(v.Embed.EmbedRecord.Record.Uri)
			}
		case v.Embed.EmbedRecordWithMedia != nil:
			p.EmbedType = "recordWithMedia"
			p.EmbedKind = "recordWithMedia"
			rwm := v.Embed.EmbedRecordWithMedia
			if rwm.Record != nil && rwm.Record.Record != nil {
				p.QuoteAuthorDID, p.QuoteRkey = splitATURI(rwm.Record.Record.Uri)
			}
			if rwm.Media != nil {
				switch {
				case rwm.Media.EmbedImages != nil:
					p.EmbedKind = "recordWithMedia:images"
					fillImages(&p, rwm.Media.EmbedImages)
				case rwm.Media.EmbedVideo != nil:
					p.EmbedKind = "recordWithMedia:video"
					fillVideo(&p, rwm.Media.EmbedVideo)
				case rwm.Media.EmbedExternal != nil:
					p.EmbedKind = "recordWithMedia:external"
					fillExternal(&p, rwm.Media.EmbedExternal)
				}
			}
		}
	}
	return p
}

func fillImages(p *PostRec, e *bsky.EmbedImages) {
	if e == nil {
		return
	}
	p.EmbedImageCount = len(e.Images)
	for _, img := range e.Images {
		if img != nil && strings.TrimSpace(img.Alt) != "" {
			p.EmbedImageWithAlt++
		}
	}
}

func fillVideo(p *PostRec, e *bsky.EmbedVideo) {
	if e == nil {
		return
	}
	p.EmbedHasVideo = true
	if e.Alt != nil && strings.TrimSpace(*e.Alt) != "" {
		p.EmbedVideoHasAlt = true
	}
}

func fillExternal(p *PostRec, e *bsky.EmbedExternal) {
	if e == nil || e.External == nil {
		return
	}
	p.EmbedExternalURI = e.External.Uri
	p.EmbedExternalTitle = e.External.Title
	p.EmbedExternalDomain = extractHost(e.External.Uri)
}

// extractHost pulls the host out of a URI. Returns "" if the URI doesn't
// look like one — by design, no error: parser-level fields are best-effort
// and downstream NULLs are fine.
func extractHost(uri string) string {
	const httpsPrefix = "https://"
	const httpPrefix = "http://"
	switch {
	case strings.HasPrefix(uri, httpsPrefix):
		uri = uri[len(httpsPrefix):]
	case strings.HasPrefix(uri, httpPrefix):
		uri = uri[len(httpPrefix):]
	default:
		return ""
	}
	if i := strings.IndexAny(uri, "/?#"); i >= 0 {
		uri = uri[:i]
	}
	return uri
}

// splitATURI takes an at-URI like "at://did:plc:abc/app.bsky.feed.post/3rkey"
// and returns (authorDID, rkey). Returns empty strings if the URI is malformed.
func splitATURI(uri string) (string, string) {
	if !strings.HasPrefix(uri, "at://") {
		return "", ""
	}
	rest := uri[5:]
	// rest looks like: did:plc:xxx/collection/rkey
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 3 {
		return "", ""
	}
	return parts[0], parts[2]
}

// parseAtTime tolerates the assorted ISO-8601 shapes ATProto clients emit.
// Returns zero time if parsing fails — the record is kept but timestamped null-ish.
func parseAtTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
