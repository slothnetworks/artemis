package manager

import "sync"

// Broadcast is a simple fan-out hub for server-sent events: subscribers receive
// every published message. Slow subscribers drop messages rather than blocking
// publishers.
type Broadcast struct {
	mu   sync.RWMutex
	subs map[chan []byte]struct{}
}

// NewBroadcast returns an empty Broadcast hub.
func NewBroadcast() *Broadcast {
	return &Broadcast{subs: make(map[chan []byte]struct{})}
}

// Subscribe registers a new subscriber and returns its channel.
func (b *Broadcast) Subscribe() chan []byte {
	ch := make(chan []byte, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (b *Broadcast) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	if _, ok := b.subs[ch]; ok {
		delete(b.subs, ch)
		close(ch)
	}
	b.mu.Unlock()
}

// Publish delivers data to every subscriber, skipping any whose buffer is full.
func (b *Broadcast) Publish(data []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- data:
		default:
		}
	}
}
