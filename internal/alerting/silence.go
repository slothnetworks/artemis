package alerting

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/slothnetworks/artemis/internal/store"
)

// Silence suppresses notifications for alerts matching its (non-empty) fields
// until Until. Empty fields match anything.
type Silence struct {
	ID     string    `json:"id"`
	Src    string    `json:"src"`
	Dst    string    `json:"dst"`
	Rule   string    `json:"rule"`
	Until  time.Time `json:"until"`
	Reason string    `json:"reason"`
	Author string    `json:"author"`
}

// Silences is a thread-safe set of active silences.
type Silences struct {
	mu   sync.RWMutex
	list []Silence
}

// NewSilences returns an empty Silences set.
func NewSilences() *Silences { return &Silences{} }

// Add registers a silence, assigning it an ID, and returns it.
func (s *Silences) Add(sl Silence) Silence {
	if sl.ID == "" {
		sl.ID = uuid.NewString()
	}
	s.mu.Lock()
	s.list = append(s.list, sl)
	s.mu.Unlock()
	return sl
}

// Remove deletes a silence by ID.
func (s *Silences) Remove(id string) {
	s.mu.Lock()
	out := s.list[:0]
	for _, sl := range s.list {
		if sl.ID != id {
			out = append(out, sl)
		}
	}
	s.list = out
	s.mu.Unlock()
}

// List returns the currently active (non-expired) silences.
func (s *Silences) List() []Silence {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Silence
	for _, sl := range s.list {
		if sl.Until.After(now) {
			out = append(out, sl)
		}
	}
	return out
}

// Matches reports whether an active silence covers the alert.
func (s *Silences) Matches(a store.Alert) bool {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sl := range s.list {
		if !sl.Until.After(now) {
			continue
		}
		if sl.Src != "" && sl.Src != a.Src {
			continue
		}
		if sl.Dst != "" && sl.Dst != a.Dst {
			continue
		}
		if sl.Rule != "" && sl.Rule != a.Rule {
			continue
		}
		return true
	}
	return false
}
