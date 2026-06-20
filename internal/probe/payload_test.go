package probe_test

import (
	"testing"

	"github.com/slothnetworks/artemis/internal/probe"
	"github.com/stretchr/testify/assert"
)

func TestNewPayload_SizeAndIntegrity(t *testing.T) {
	for _, size := range []int{8, 64, 1500, 9000, 65000} {
		p := probe.NewPayload(size, 42)
		assert.Len(t, p, size)

		declared, actual, ok := probe.ValidatePayload(p)
		assert.True(t, ok, "size %d should validate", size)
		assert.Equal(t, size, declared)
		assert.Equal(t, size, actual)
	}
}

func TestNewPayload_BelowHeaderClampsUp(t *testing.T) {
	p := probe.NewPayload(2, 1)
	assert.Len(t, p, 8) // clamped up to the header length
	_, _, ok := probe.ValidatePayload(p)
	assert.True(t, ok)
}

func TestNewPayload_Deterministic(t *testing.T) {
	assert.Equal(t, probe.NewPayload(128, 7), probe.NewPayload(128, 7))
	assert.NotEqual(t, probe.NewPayload(128, 7), probe.NewPayload(128, 8))
}

func TestValidatePayload_DetectsModification(t *testing.T) {
	p := []byte(probe.NewPayload(64, 5))

	// Truncation → declared length no longer matches actual.
	_, _, ok := probe.ValidatePayload(string(p[:50]))
	assert.False(t, ok)

	// Body corruption at the same length → CRC mismatch.
	corrupt := make([]byte, len(p))
	copy(corrupt, p)
	corrupt[40] ^= 0xFF
	_, _, ok = probe.ValidatePayload(string(corrupt))
	assert.False(t, ok)
}

func TestValidatePayload_TooShort(t *testing.T) {
	_, _, ok := probe.ValidatePayload("ab")
	assert.False(t, ok)
}
