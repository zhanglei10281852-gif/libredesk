package inbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	conversationmodels "github.com/abhinavxd/libredesk/internal/conversation/models"
	imodels "github.com/abhinavxd/libredesk/internal/inbox/models"
	"github.com/abhinavxd/libredesk/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/zerodha/logf"
)

// fakeRuntime is a deterministic Inbox used to verify generation handoffs
// without opening any network resource.
type fakeRuntime struct {
	id        int
	name      string
	channel   string
	closeCnt  atomic.Int32
	receiving atomic.Bool
	peakLive  *atomic.Int32

	eventsMu *sync.Mutex
	events   *[]string
}

func newFakeRuntime(id int, name string, events *[]string, eventsMu *sync.Mutex, peak *atomic.Int32) *fakeRuntime {
	return &fakeRuntime{id: id, name: name, channel: ChannelEmail, eventsMu: eventsMu, events: events, peakLive: peak}
}

func (f *fakeRuntime) push(event string) {
	f.eventsMu.Lock()
	*f.events = append(*f.events, f.name+":"+event)
	f.eventsMu.Unlock()
}

func (f *fakeRuntime) Close() error {
	f.closeCnt.Add(1)
	return nil
}
func (f *fakeRuntime) Identifier() int { return f.id }
func (f *fakeRuntime) Receive(ctx context.Context) error {
	f.receiving.Store(true)
	defer f.receiving.Store(false)
	f.push("enter")
	f.peakLive.Add(1)
	defer f.peakLive.Add(-1)
	<-ctx.Done()
	f.push("exit")
	return nil
}
func (f *fakeRuntime) Send(conversationmodels.OutboundMessage) error { return nil }
func (f *fakeRuntime) Name() string                                  { return f.name }
func (f *fakeRuntime) FromAddress() string                           { return "support@example.com" }
func (f *fakeRuntime) FromNameTemplate() string                      { return "" }
func (f *fakeRuntime) ReplyToAddress() string                        { return "" }
func (f *fakeRuntime) Channel() string                               { return f.channel }

// liveTracker records receiver start/stop order and the peak number of
// concurrently running receivers across all generations of one inbox.
type liveTracker struct {
	mu     sync.Mutex
	events []string
	peak   atomic.Int32
}

func newSwitchTestManager(t *testing.T) *Manager {
	t.Helper()
	db := testutil.NewDB(t, "inbox_runtime_switch")
	lo := logf.New(logf.Opts{})
	mgr, err := New(&lo, db, testutil.NewI18n(t), "01234567890123456789012345678901")
	require.NoError(t, err)
	return mgr
}

func emailPatch(name string) imodels.Inbox {
	cfg, _ := json.Marshal(imodels.Config{
		IMAP: []imodels.IMAPConfig{{Host: "imap.example.com", Port: 993, Mailbox: "INBOX", TLSType: "tls", ReadInterval: "1m"}},
		SMTP: []imodels.SMTPConfig{{Host: "smtp.example.com", Port: 587, AuthProtocol: "plain"}},
	})
	return imodels.Inbox{
		Name:    name,
		Channel: ChannelEmail,
		From:    "support@example.com",
		Enabled: true,
		Config:  cfg,
	}
}

// requireReceiverIdle ensures the fake runtime's receiver goroutine has stopped.
func requireReceiverStopped(t *testing.T, rt *fakeRuntime) {
	t.Helper()
	require.Eventually(t, func() bool { return !rt.receiving.Load() }, time.Second, 2*time.Millisecond)
}

func requireReceiverRunning(t *testing.T, rt *fakeRuntime) {
	t.Helper()
	require.Eventually(t, func() bool { return rt.receiving.Load() }, time.Second, 2*time.Millisecond)
}

// TestUpdateFailedCandidateKeepsEverything verifies that when the candidate
// cannot initialize, nothing is committed and the old runtime keeps serving.
func TestUpdateFailedCandidateKeepsEverything(t *testing.T) {
	mgr := newSwitchTestManager(t)
	ctx := context.Background()

	created, err := mgr.Create(emailPatch("Old name"))
	require.NoError(t, err)

	var (
		tracker liveTracker
		old     = newFakeRuntime(created.ID, "old", &tracker.events, &tracker.mu, &tracker.peak)
	)
	mgr.mu.Lock()
	mgr.inboxes[created.ID] = old
	mgr.startReceiver(ctx, old)
	mgr.mu.Unlock()
	requireReceiverRunning(t, old)

	failing := newFakeRuntime(created.ID, "failed", &tracker.events, &tracker.mu, &tracker.peak)
	initFn := func(imodels.Inbox, MessageStore, UserStore) (Inbox, error) {
		// Constructor created resources, then discovered it is unusable.
		return failing, fmt.Errorf("smtp auth protocol rejected")
	}

	_, err = mgr.Update(ctx, created.ID, emailPatch("New name"), initFn)
	require.Error(t, err)
	require.Contains(t, err.Error(), "smtp auth protocol rejected")
	require.Contains(t, err.Error(), "previous configuration")

	// The failed candidate's resources are closed exactly once.
	require.Equal(t, int32(1), failing.closeCnt.Load())
	// The old runtime is untouched and still receiving.
	active, err := mgr.Get(created.ID)
	require.NoError(t, err)
	require.Same(t, old, active)
	require.True(t, old.receiving.Load())
	require.Equal(t, int32(0), old.closeCnt.Load())

	// DB: name, config and generation are all at the pre-save state.
	rec, err := mgr.GetDBRecord(created.ID)
	require.NoError(t, err)
	require.Equal(t, "Old name", rec.Name)
	require.Equal(t, int64(0), rec.RuntimeGeneration)

	mgr.Close()
	require.Eventually(t, func() bool { return old.closeCnt.Load() == 1 }, time.Second, time.Millisecond)
}

// TestUpdateHotSwapGeneration verifies the committed generation and the unique
// active runtime always match and that receivers never overlap.
func TestUpdateHotSwapGeneration(t *testing.T) {
	mgr := newSwitchTestManager(t)
	ctx := context.Background()

	created, err := mgr.Create(emailPatch("Gen0"))
	require.NoError(t, err)

	var tracker liveTracker
	cands := make([]*fakeRuntime, 0, 3)
	initFn := func(rec imodels.Inbox, _ MessageStore, _ UserStore) (Inbox, error) {
		rt := newFakeRuntime(rec.ID, rec.Name, &tracker.events, &tracker.mu, &tracker.peak)
		cands = append(cands, rt)
		return rt, nil
	}

	// First update: no old runtime, generation 1 starts.
	upd1, err := mgr.Update(ctx, created.ID, emailPatch("Gen1"), initFn)
	require.NoError(t, err)
	require.Equal(t, int64(1), upd1.RuntimeGeneration)
	require.Len(t, cands, 1)
	requireReceiverRunning(t, cands[0])

	active, err := mgr.Get(created.ID)
	require.NoError(t, err)
	require.Same(t, cands[0], active)

	// Second update: candidate takes over with generation 2, gen1 closes once.
	upd2, err := mgr.Update(ctx, created.ID, emailPatch("Gen2"), initFn)
	require.NoError(t, err)
	require.Equal(t, int64(2), upd2.RuntimeGeneration)
	require.Len(t, cands, 2)
	requireReceiverStopped(t, cands[0])
	requireReceiverRunning(t, cands[1])
	require.Eventually(t, func() bool { return cands[0].closeCnt.Load() == 1 }, time.Second, time.Millisecond)

	active, err = mgr.Get(created.ID)
	require.NoError(t, err)
	require.Same(t, cands[1], active)
	require.Equal(t, int32(0), cands[1].closeCnt.Load())

	// DB and the tracked generation agree with the unique active runtime.
	rec, err := mgr.GetDBRecord(created.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), rec.RuntimeGeneration)
	require.Equal(t, int64(2), mgr.runtimeGen[created.ID])
	mgr.mu.RLock()
	_, hasReceiver := mgr.receivers[created.ID]
	nActive := len(mgr.inboxes)
	mgr.mu.RUnlock()
	require.True(t, hasReceiver)
	require.Equal(t, 1, nActive, "only one active runtime must exist")

	// No receiver overlap at any point of the two handoffs.
	require.Eventually(t, func() bool {
		return cands[1].receiving.Load() && tracker.peak.Load() == 1
	}, time.Second, 2*time.Millisecond)
	require.Equal(t, int32(1), tracker.peak.Load(), "two receivers must never run concurrently")

	// Old receiver stopped before the new receiver started.
	tracker.mu.Lock()
	events := append([]string(nil), tracker.events...)
	tracker.mu.Unlock()
	stopGen1 := indexOf(events, "Gen1:exit")
	startGen2 := indexOf(events, "Gen2:enter")
	require.GreaterOrEqual(t, stopGen1, 0)
	require.GreaterOrEqual(t, startGen2, 0)
	require.Less(t, stopGen1, startGen2)

	mgr.Close()
	require.Eventually(t, func() bool { return cands[1].closeCnt.Load() == 1 }, time.Second, time.Millisecond)
}

// TestUpdatesSerializedPerInbox runs two concurrent updates and verifies the
// later-committed generation always wins regardless of scheduling.
func TestUpdatesSerializedPerInbox(t *testing.T) {
	mgr := newSwitchTestManager(t)
	ctx := context.Background()

	created, err := mgr.Create(emailPatch("Gen0"))
	require.NoError(t, err)

	var (
		tracker     liveTracker
		records     = make(chan string, 2)
		built       = make(chan *fakeRuntime, 2)
		releaseSlow = make(chan struct{})
	)
	initFn := func(rec imodels.Inbox, _ MessageStore, _ UserStore) (Inbox, error) {
		rt := newFakeRuntime(rec.ID, rec.Name, &tracker.events, &tracker.mu, &tracker.peak)
		built <- rt
		records <- rec.Name
		if rec.Name == "Slow" {
			<-releaseSlow
		}
		return rt, nil
	}

	errs := make(chan error, 2)
	go func() { _, e := mgr.Update(ctx, created.ID, emailPatch("Slow"), initFn); errs <- e }()
	first := <-built
	go func() { _, e := mgr.Update(ctx, created.ID, emailPatch("Fast"), initFn); errs <- e }()

	// The fast update is serialized behind the slow one; let it notice, then
	// release the slow candidate.
	select {
	case <-built:
		t.Fatalf("second update must not construct a candidate before the first generation committed")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseSlow)
	second := <-built
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, "Slow", first.name)
	require.Equal(t, "Fast", second.name)

	rec, err := mgr.GetDBRecord(created.ID)
	require.NoError(t, err)
	require.Equal(t, "Fast", rec.Name)
	require.Equal(t, int64(2), rec.RuntimeGeneration)
	require.Equal(t, int64(2), mgr.runtimeGen[created.ID])
	active, err := mgr.Get(created.ID)
	require.NoError(t, err)
	require.Same(t, second, active)
	mgr.Close()
}

// TestHandoffRejectsStaleGeneration verifies an older candidate can never
// replace a newer committed generation.
func TestHandoffRejectsStaleGeneration(t *testing.T) {
	mgr := newSwitchTestManager(t)
	ctx := context.Background()

	created, err := mgr.Create(emailPatch("Gen0"))
	require.NoError(t, err)

	var tracker liveTracker
	current := newFakeRuntime(created.ID, "current", &tracker.events, &tracker.mu, &tracker.peak)
	mgr.mu.Lock()
	mgr.inboxes[created.ID] = current
	mgr.runtimeGen[created.ID] = 5
	mgr.mu.Unlock()

	stale := newFakeRuntime(created.ID, "stale", &tracker.events, &tracker.mu, &tracker.peak)
	err = mgr.handoff(ctx, stale, 4)
	require.Error(t, err)
	require.Equal(t, int32(1), stale.closeCnt.Load())
	active, err := mgr.Get(created.ID)
	require.NoError(t, err)
	require.Same(t, current, active)
	require.Equal(t, int64(5), mgr.runtimeGen[created.ID])
}

// TestToggleAndDeleteRuntimeLifecycle verifies disable/delete intentionally
// stop the runtime and re-enable starts exactly one receiver.
func TestToggleAndDeleteRuntimeLifecycle(t *testing.T) {
	mgr := newSwitchTestManager(t)
	ctx := context.Background()

	created, err := mgr.Create(emailPatch("Gen0"))
	require.NoError(t, err)

	var tracker liveTracker
	makeRt := func(rec imodels.Inbox, _ MessageStore, _ UserStore) (Inbox, error) {
		return newFakeRuntime(rec.ID, rec.Name, &tracker.events, &tracker.mu, &tracker.peak), nil
	}

	require.NoError(t, mgr.StartInbox(ctx, created.ID, makeRt))
	first, err := mgr.Get(created.ID)
	require.NoError(t, err)
	firstFake := first.(*fakeRuntime)
	requireReceiverRunning(t, firstFake)

	// Disable: runtime removed, closed, receiver gone.
	disabled, err := mgr.Toggle(ctx, created.ID, makeRt)
	require.NoError(t, err)
	require.False(t, disabled.Enabled)
	_, err = mgr.Get(created.ID)
	require.ErrorIs(t, err, ErrInboxNotFound)
	requireReceiverStopped(t, firstFake)
	require.Eventually(t, func() bool { return firstFake.closeCnt.Load() == 1 }, time.Second, time.Millisecond)
	mgr.mu.RLock()
	_, hasReceiver := mgr.receivers[created.ID]
	_, hasGen := mgr.runtimeGen[created.ID]
	mgr.mu.RUnlock()
	require.False(t, hasReceiver)
	require.False(t, hasGen)

	// Re-enable: one new receiver, the old object stays closed.
	enabled, err := mgr.Toggle(ctx, created.ID, makeRt)
	require.NoError(t, err)
	require.True(t, enabled.Enabled)
	second, err := mgr.Get(created.ID)
	require.NoError(t, err)
	require.NotSame(t, first, second)
	requireReceiverRunning(t, second.(*fakeRuntime))
	require.Equal(t, int32(1), firstFake.closeCnt.Load())
	require.Equal(t, int32(1), tracker.peak.Load())

	// Delete: runtime stopped and record gone.
	require.NoError(t, mgr.SoftDelete(created.ID))
	_, err = mgr.Get(created.ID)
	require.ErrorIs(t, err, ErrInboxNotFound)
	_, err = mgr.GetDBRecord(created.ID)
	require.Error(t, err)
	mgr.Close()
}

func indexOf(items []string, want string) int {
	for i, v := range items {
		if v == want {
			return i
		}
	}
	return -1
}
