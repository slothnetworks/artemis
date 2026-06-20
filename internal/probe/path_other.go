//go:build !linux

package probe

import "net"

// inspectPath is a no-op on non-Linux platforms; path MTU / MSS introspection
// relies on Linux socket options. Detection falls back to the size-sweep
// behavioral inference.
func inspectPath(_ net.Conn) (pathMTU, effMSS int) { return 0, 0 }
