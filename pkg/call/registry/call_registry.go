package call_registry

import (
	"sync"

	"github.com/purpshell/meowcaller"
)

// key is (instance, call ID), not the call ID alone: the same WhatsApp number can be
// paired to several instances on this server (several apps share it), and every one of
// them sees the SAME call ID for the same call. Keyed by call ID alone, the last instance
// to capture the offer overwrote the others, and answer/stream from the instance the
// caller actually used failed with "no pending call with that id".
type key struct {
	instanceID string
	callID     string
}

// CallRegistry tracks in-progress meowcaller calls per instance. It is written from the
// whatsmeow event-handling goroutine (on incoming call) and read/written from HTTP
// handler goroutines (answer/hangup/stream), so all access is mutex-guarded.
type CallRegistry struct {
	mu      sync.RWMutex
	entries map[key]*meowcaller.Call
}

// NewCallRegistry returns an empty registry.
func NewCallRegistry() *CallRegistry {
	return &CallRegistry{entries: make(map[key]*meowcaller.Call)}
}

// Store records call under (instanceID, call.ID()).
func (r *CallRegistry) Store(instanceID string, call *meowcaller.Call) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[key{instanceID, call.ID()}] = call
}

// Get returns the call instanceID stored under callID.
func (r *CallRegistry) Get(instanceID, callID string) (*meowcaller.Call, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	call, ok := r.entries[key{instanceID, callID}]
	return call, ok
}

// Delete removes instanceID's entry for callID; other instances' entries stay.
func (r *CallRegistry) Delete(instanceID, callID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, key{instanceID, callID})
}
