//go:build linux

package probe

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// inspectPath best-effort reads the effective TCP MSS and the kernel-discovered
// path MTU for a connection using Linux socket options, in-process and without
// raw sockets or extra privileges. Either value may be 0 when unavailable (e.g.
// MSS on a UDP socket, or path MTU before PMTU discovery has run).
func inspectPath(conn net.Conn) (pathMTU, effMSS int) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return 0, 0
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return 0, 0
	}
	_ = raw.Control(func(fd uintptr) {
		if mss, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_MAXSEG); err == nil && mss > 0 {
			effMSS = mss
		}
		if mtu, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_MTU); err == nil && mtu > 0 {
			pathMTU = mtu
		}
	})
	return pathMTU, effMSS
}
