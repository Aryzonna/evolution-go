package call_registry

import (
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestStoreAndGet(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}

	r.Store("instance-a", call)

	got, ok := r.Get("instance-a", callIDOf(call))
	if !ok {
		t.Fatal("expected call to be found")
	}
	if got != call {
		t.Fatal("expected the same call pointer back")
	}
}

func TestGetWrongInstanceFails(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}
	r.Store("instance-a", call)

	_, ok := r.Get("instance-b", callIDOf(call))
	if ok {
		t.Fatal("expected lookup from a different instance to fail")
	}
}

func TestDeleteRemovesEntry(t *testing.T) {
	r := NewCallRegistry()
	call := &meowcaller.Call{}
	r.Store("instance-a", call)
	r.Delete("instance-a", callIDOf(call))

	_, ok := r.Get("instance-a", callIDOf(call))
	if ok {
		t.Fatal("expected entry to be gone after Delete")
	}
}

// The same number paired to several instances: every instance sees the same call ID,
// and each must keep its own entry (before, the last capture overwrote the others).
func TestSameCallIDOnTwoInstances(t *testing.T) {
	r := NewCallRegistry()
	a := &meowcaller.Call{}
	b := &meowcaller.Call{}
	r.Store("instance-a", a)
	r.Store("instance-b", b)

	if got, ok := r.Get("instance-a", callIDOf(a)); !ok || got != a {
		t.Fatal("instance-a lost its call when instance-b stored the same call ID")
	}
	if got, ok := r.Get("instance-b", callIDOf(b)); !ok || got != b {
		t.Fatal("instance-b should find its own call")
	}

	r.Delete("instance-b", callIDOf(b))
	if _, ok := r.Get("instance-a", callIDOf(a)); !ok {
		t.Fatal("deleting instance-b's entry must not remove instance-a's")
	}
}

// callIDOf mirrors what CallRegistry.Store keys entries by: meowcaller.Call.ID().
// A zero-value *meowcaller.Call has an empty string ID, which is a perfectly valid
// (if degenerate) key for exercising Store/Get/Delete without a live call.
func callIDOf(call *meowcaller.Call) string {
	return call.ID()
}
