package tsm1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestDefaultHotKeyWarn_HashOnlyByDefault verifies the default hot-key report
// identifies the key only by a truncated SHA-256 hash: no raw key bytes may
// appear anywhere in the emitted entry (series keys can contain sensitive tag
// and field names).
func TestHotKeyWarn_HashOnlyByDefault(t *testing.T) {
	defer func(prev bool) { hotKeyWarnDisabled = prev }(hotKeyWarnDisabled)
	hotKeyWarnDisabled = false
	defer func(prev bool) { hotKeyWarnRawKey = prev }(hotKeyWarnRawKey)
	hotKeyWarnRawKey = false

	key := []byte("cpu,host=secret-host-42,region=us-east-1")
	rawKey := string(key)
	sum := sha256.Sum256(key)
	wantHash := hex.EncodeToString(sum[:4])

	core, logs := observer.New(zapcore.WarnLevel)
	defaultHotKeyWarn(zap.New(core), key, hotKeyGatherWarnBytes+1)

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, ok := fields["key"]; ok {
		t.Fatalf("default report leaked the raw key: %v", got)
	}
	if got := fields["key_hash"]; got != wantHash {
		t.Fatalf("key_hash = %v, want %q (first 8 hex chars of the key's SHA-256)", got, wantHash)
	}
	if got := fields["total_bytes"]; got != uint64(hotKeyGatherWarnBytes+1) {
		t.Fatalf("total_bytes = %v, want %d", got, uint64(hotKeyGatherWarnBytes+1))
	}

	// Defense in depth: no part of the entry may contain the raw key bytes.
	var sb strings.Builder
	sb.WriteString(entries[0].Message)
	for k, v := range fields {
		sb.WriteString(k)
		sb.WriteString(fmt.Sprint(v))
	}
	if strings.Contains(sb.String(), rawKey) {
		t.Fatalf("default report leaked raw key bytes: %q", sb.String())
	}
}

// TestDefaultHotKeyWarn_RawKeyOptIn verifies the raw (truncated) key is
// emitted only when the hotKeyWarnRawKey opt-in is set.
func TestHotKeyWarn_RawKeyOptIn(t *testing.T) {
	defer func(prev bool) { hotKeyWarnDisabled = prev }(hotKeyWarnDisabled)
	hotKeyWarnDisabled = false
	defer func(prev bool) { hotKeyWarnRawKey = prev }(hotKeyWarnRawKey)
	hotKeyWarnRawKey = true

	key := []byte("cpu,host=secret-host-42,region=us-east-1")
	core, logs := observer.New(zapcore.WarnLevel)
	defaultHotKeyWarn(zap.New(core), key, hotKeyGatherWarnBytes+1)

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
	}
	if got := entries[0].ContextMap()["key"]; got != string(key) {
		t.Fatalf("key = %v, want %q", got, string(key))
	}
}

// TestDefaultHotKeyWarn_NilLogger verifies a nil logger is a safe no-op.
func TestHotKeyWarn_NilLogger(t *testing.T) {
	defer func(prev bool) { hotKeyWarnDisabled = prev }(hotKeyWarnDisabled)
	hotKeyWarnDisabled = false
	defaultHotKeyWarn(nil, []byte("cpu,host=a"), hotKeyGatherWarnBytes+1)
}

// TestCheckHotKeyGather_Uint64Accounting pins the gather accounting type: the
// total is accumulated and reported as uint64, so a single-key gather past
// math.MaxInt32 (the 32-bit int ceiling) is reported instead of wrapping
// negative and skipping the warning. A small gather stays below
// hotKeyGatherWarnBytes and must not fire the hook.
func TestHotKeyGather_Uint64Accounting(t *testing.T) {
	// Compile-time: the hook's total parameter is uint64.
	var _ func(lg *zap.Logger, key []byte, total uint64) = hotKeyWarnFn

	var fired bool
	var got uint64
	prev := hotKeyWarnFn
	defer func() { hotKeyWarnFn = prev }()
	hotKeyWarnFn = func(lg *zap.Logger, key []byte, total uint64) {
		fired, got = true, total
	}

	checkHotKeyGather(zap.NewNop(), []byte("m"), blocks{{b: make([]byte, 64)}, {b: make([]byte, 32)}})
	if fired {
		t.Fatalf("hook fired below threshold: total %d, threshold %d", got, uint64(hotKeyGatherWarnBytes))
	}
}
