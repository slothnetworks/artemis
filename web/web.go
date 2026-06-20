// Package web holds the embedded UI assets (templates + static files). Embedding
// here lets the manager binary ship fully self-contained — no web files on disk
// at runtime.
package web

import "embed"

// FS contains the HTML templates and static assets.
//
//go:embed templates static
var FS embed.FS
