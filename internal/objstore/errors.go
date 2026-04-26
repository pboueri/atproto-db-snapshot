package objstore

import "errors"

// ErrNotExist is returned by Stat / Get when a path is not present in the
// store. It is exposed so callers can probe with errors.Is rather than
// matching error strings.
var ErrNotExist = errors.New("objstore: object does not exist")
