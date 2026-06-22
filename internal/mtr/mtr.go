// Package mtr is a pure-Go, raw-socket MTR/traceroute engine. It sends ICMP Echo
// probes at increasing TTLs, matches the Time-Exceeded / Echo-Reply responses
// back to per-hop RTT/loss statistics, and returns a single report.
//
// It is adapted, with the author's approval, from the native MTR engine in the
// sibling uptime-monitor project (the continuous loop, L4/UDP-TCP transports and
// zerolog dependency were dropped; this is a one-shot ICMP capture). Requires
// CAP_NET_RAW. Supports IPv4 and IPv6.
package mtr

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

const (
	maxHops      = 30
	probeTimeout = 1500 * time.Millisecond
	payloadSize  = 32
	intervalMS   = 250
)

var idSeq uint32

// Hop is the per-hop result for one TTL.
type Hop struct {
	Number   int     `json:"hop"`
	IP       string  `json:"ip"`
	LossPct  float64 `json:"loss_pct"`
	Sent     int     `json:"sent"`
	LastMs   float64 `json:"last_ms"`
	AvgMs    float64 `json:"avg_ms"`
	BestMs   float64 `json:"best_ms"`
	WorstMs  float64 `json:"worst_ms"`
	StddevMs float64 `json:"stddev_ms"`
}

// Result is one MTR report toward a target.
type Result struct {
	Target             string        `json:"target"`
	DestinationReached bool          `json:"destination_reached"`
	Hops               []Hop         `json:"hops"`
	Duration           time.Duration `json:"duration"`
}

type hopAcc struct {
	ip   string
	sent int
	recv int
	rtts []float64
	last float64
}

type pendingProbe struct {
	ttl    int
	sentAt time.Time
}

// Capture runs a single ICMP MTR report toward target over `cycles` rounds.
func Capture(ctx context.Context, target string, cycles int) (Result, error) {
	if cycles <= 0 {
		cycles = 3
	}
	res := Result{Target: target}
	start := time.Now()

	ipAddr, isV6, err := resolveTarget(target)
	if err != nil {
		return res, fmt.Errorf("resolve %s: %w", target, err)
	}

	network, proto := "ip4:icmp", 1
	if isV6 {
		network, proto = "ip6:ipv6-icmp", 58
	}
	conn, err := icmp.ListenPacket(network, "")
	if err != nil {
		return res, fmt.Errorf("listen %s (need CAP_NET_RAW): %w", network, err)
	}
	if (isV6 && conn.IPv6PacketConn() == nil) || (!isV6 && conn.IPv4PacketConn() == nil) {
		_ = conn.Close()
		return res, fmt.Errorf("no %s packet conn", network)
	}
	id := uint16(atomic.AddUint32(&idSeq, 1) + uint32(rand.Intn(1<<10)))
	t := &icmpTransport{conn: conn, isV6: isV6, proto: proto, id: id, dst: &net.IPAddr{IP: ipAddr}}

	var (
		mu      sync.Mutex
		hops    = make([]hopAcc, maxHops+1)
		pending = make(map[uint16]pendingProbe)
		destTTL int
		seq     uint16
		readWG  sync.WaitGroup
	)

	readWG.Add(1)
	go func() {
		defer readWG.Done()
		t.readLoop(func(s uint16, hopIP string, destReached bool) {
			mu.Lock()
			defer mu.Unlock()
			p, known := pending[s]
			if !known {
				return
			}
			delete(pending, s)
			elapsed := time.Since(p.sentAt)
			if elapsed > probeTimeout {
				return
			}
			rtt := elapsed.Seconds() * 1000.0
			h := &hops[p.ttl]
			h.recv++
			h.rtts = append(h.rtts, rtt)
			h.last = rtt
			if h.ip == "" {
				h.ip = hopIP
			}
			if destReached && (destTTL == 0 || p.ttl < destTTL) {
				destTTL = p.ttl
			}
		})
	}()

sendLoop:
	for round := 0; round < cycles; round++ {
		if ctx.Err() != nil {
			break
		}
		roundStart := time.Now()

		mu.Lock()
		maxTTL := maxHops
		if destTTL != 0 {
			maxTTL = destTTL
		}
		mu.Unlock()

		for ttl := 1; ttl <= maxTTL; ttl++ {
			if ctx.Err() != nil {
				break sendLoop
			}
			seq++
			s := seq
			mu.Lock()
			pending[s] = pendingProbe{ttl: ttl, sentAt: time.Now()}
			hops[ttl].sent++
			mu.Unlock()
			_ = t.sendProbe(ttl, s)
		}

		if round < cycles-1 {
			if wait := time.Duration(intervalMS)*time.Millisecond - time.Since(roundStart); wait > 0 {
				select {
				case <-ctx.Done():
					break sendLoop
				case <-time.After(wait):
				}
			}
		}
	}

	select {
	case <-ctx.Done():
	case <-time.After(probeTimeout):
	}
	t.close()
	readWG.Wait()

	res.Hops = buildHops(hops, destTTL)
	res.DestinationReached = destTTL != 0
	res.Duration = time.Since(start)
	if len(res.Hops) == 0 {
		return res, fmt.Errorf("no hops responded")
	}
	return res, nil
}

type icmpTransport struct {
	conn    *icmp.PacketConn
	isV6    bool
	proto   int
	id      uint16
	dst     net.Addr
	closing atomic.Bool
}

func (t *icmpTransport) sendProbe(ttl int, seq uint16) error {
	if err := setTTL(t.conn, t.isV6, ttl); err != nil {
		return err
	}
	wm := icmp.Message{
		Type: echoType(t.isV6),
		Code: 0,
		Body: &icmp.Echo{ID: int(t.id), Seq: int(seq), Data: make([]byte, payloadSize)},
	}
	wb, err := wm.Marshal(nil)
	if err != nil {
		return err
	}
	_, err = t.conn.WriteTo(wb, t.dst)
	return err
}

func (t *icmpTransport) readLoop(onReply func(uint16, string, bool)) {
	buf := make([]byte, 1500)
	for {
		n, peer, err := t.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		msg, err := icmp.ParseMessage(t.proto, buf[:n])
		if err != nil {
			continue
		}
		gotID, gotSeq, ok := matchedProbe(msg, t.isV6)
		if !ok || gotID != t.id {
			continue
		}
		onReply(uint16(gotSeq), peer.String(), isEchoReply(msg))
	}
}

func (t *icmpTransport) close() {
	t.closing.Store(true)
	_ = t.conn.SetReadDeadline(time.Now())
	_ = t.conn.Close()
}

func buildHops(hops []hopAcc, destTTL int) []Hop {
	last := destTTL
	if last == 0 {
		for ttl := maxHops; ttl >= 1; ttl-- {
			if hops[ttl].recv > 0 {
				last = ttl
				break
			}
		}
	}
	if last == 0 {
		return nil
	}
	out := make([]Hop, 0, last)
	for ttl := 1; ttl <= last; ttl++ {
		h := hops[ttl]
		hop := Hop{Number: ttl, Sent: h.sent}
		if h.recv == 0 || h.ip == "" {
			hop.LossPct = 100
			out = append(out, hop)
			continue
		}
		hop.IP = h.ip
		if h.sent > 0 {
			hop.LossPct = float64(h.sent-h.recv) / float64(h.sent) * 100
			if hop.LossPct < 0 {
				hop.LossPct = 0
			}
		}
		hop.LastMs = h.last
		hop.BestMs, hop.WorstMs, hop.AvgMs, hop.StddevMs = rttStats(h.rtts)
		out = append(out, hop)
	}
	return out
}

func rttStats(rtts []float64) (best, worst, avg, stddev float64) {
	if len(rtts) == 0 {
		return 0, 0, 0, 0
	}
	best, worst = rtts[0], rtts[0]
	sum := 0.0
	for _, v := range rtts {
		if v < best {
			best = v
		}
		if v > worst {
			worst = v
		}
		sum += v
	}
	avg = sum / float64(len(rtts))
	var sq float64
	for _, v := range rtts {
		d := v - avg
		sq += d * d
	}
	return best, worst, avg, math.Sqrt(sq / float64(len(rtts)))
}

func resolveTarget(target string) (net.IP, bool, error) {
	if ip := net.ParseIP(target); ip != nil {
		return ip, ip.To4() == nil, nil
	}
	addr, err := net.ResolveIPAddr("ip", target)
	if err != nil {
		return nil, false, err
	}
	return addr.IP, addr.IP.To4() == nil, nil
}

func echoType(isV6 bool) icmp.Type {
	if isV6 {
		return ipv6.ICMPTypeEchoRequest
	}
	return ipv4.ICMPTypeEcho
}

func setTTL(conn *icmp.PacketConn, isV6 bool, ttl int) error {
	if isV6 {
		return conn.IPv6PacketConn().SetHopLimit(ttl)
	}
	return conn.IPv4PacketConn().SetTTL(ttl)
}

func isEchoReply(msg *icmp.Message) bool {
	switch msg.Type {
	case ipv4.ICMPTypeEchoReply, ipv6.ICMPTypeEchoReply:
		return true
	}
	return false
}

func matchedProbe(msg *icmp.Message, isV6 bool) (id uint16, seq int, ok bool) {
	switch body := msg.Body.(type) {
	case *icmp.Echo:
		if isEchoReply(msg) {
			return uint16(body.ID), body.Seq, true
		}
	case *icmp.TimeExceeded:
		return embeddedEchoIDSeq(body.Data, isV6)
	case *icmp.DstUnreach:
		return embeddedEchoIDSeq(body.Data, isV6)
	}
	return 0, 0, false
}

func embeddedEchoIDSeq(data []byte, isV6 bool) (uint16, int, bool) {
	ipHdrLen := 40
	if !isV6 {
		if len(data) < 1 {
			return 0, 0, false
		}
		ipHdrLen = int(data[0]&0x0f) * 4
	}
	if len(data) < ipHdrLen+8 {
		return 0, 0, false
	}
	icmpHdr := data[ipHdrLen:]
	return binary.BigEndian.Uint16(icmpHdr[4:6]), int(binary.BigEndian.Uint16(icmpHdr[6:8])), true
}
