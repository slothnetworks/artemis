package probe

import (
	"encoding/binary"
	"hash/crc32"
)

// payloadHeaderLen is the size of the self-describing payload header: a uint32
// declared total length followed by a uint32 CRC32 of the body.
const payloadHeaderLen = 8

// NewPayload builds a self-describing probe payload of exactly size bytes (or
// payloadHeaderLen if size is smaller). The payload embeds its own declared
// length and a CRC32 of its body so either end can detect in-flight
// modification (truncation, padding, or corruption) — see ValidatePayload.
//
// The body is filled deterministically from seed so identical probes are
// reproducible and the bytes are not trivially uniform.
func NewPayload(size int, seed uint64) string {
	if size < payloadHeaderLen {
		size = payloadHeaderLen
	}
	buf := make([]byte, size)
	body := buf[payloadHeaderLen:]

	// Deterministic xorshift fill so the body has structure (not all zeros).
	x := seed | 1
	for i := range body {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		body[i] = byte(x)
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(size))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(body))
	return string(buf)
}

// ValidatePayload checks a received payload against its own embedded header,
// returning the declared length, the actual length, and whether the payload is
// intact (actual == declared and the body CRC matches). A payload too short to
// hold a header is reported as not-ok.
//
// This is the destination-side ("forward path") check: it needs only the
// received bytes, not the original.
func ValidatePayload(data string) (declared, actual int, ok bool) {
	b := []byte(data)
	actual = len(b)
	if actual < payloadHeaderLen {
		return actual, actual, false
	}
	declared = int(binary.BigEndian.Uint32(b[0:4]))
	crcStored := binary.BigEndian.Uint32(b[4:8])
	crcCalc := crc32.ChecksumIEEE(b[payloadHeaderLen:])
	return declared, actual, actual == declared && crcCalc == crcStored
}
