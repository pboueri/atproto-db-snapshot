package bootstrap

import "github.com/pboueri/atproto-db-snapshot/internal/intern"

// didInterner exists only so store.go can avoid taking a public dependency on
// internal/intern. Keeping it as a function pointer also lets tests swap in a
// stub if they ever need to.
var didInterner = intern.DIDID
