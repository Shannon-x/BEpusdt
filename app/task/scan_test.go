package task

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/smallnest/chanx"
	applog "github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "bepusdt-task-test")
	if err != nil {
		panic(err)
	}
	if err := applog.Init(filepath.Join(dir, "logs")); err != nil {
		panic(err)
	}

	// 真实的临时 SQLite：游标、任务、流水、outbox 都会落库
	dbPath := filepath.Join(dir, "task-test.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?cache=shared&mode=rwc&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		panic(err)
	}
	model.Db = db
	if err := model.AutoMigrate(); err != nil {
		panic(err)
	}
	model.FillDefaultConf()
	model.RefreshC()

	// 测试中把重试延迟压到毫秒级，并录制而不是发送告警
	scanRetryBaseDelay = time.Millisecond
	alertSender = func(string, string) {}

	code := m.Run()
	applog.Close()
	model.Close()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type recordedAlert struct {
	title string
	text  string
}

// recordAlerts 在测试期间录制告警，结束后恢复
func recordAlerts(t *testing.T) *[]recordedAlert {
	t.Helper()
	alerts := make([]recordedAlert, 0)
	old := alertSender
	alertSender = func(title, text string) {
		alerts = append(alerts, recordedAlert{title: title, text: text})
	}
	t.Cleanup(func() { alertSender = old })

	return &alerts
}

func TestEndpointPickerSwitchesOnlyWhenFailedNodeIsCurrent(t *testing.T) {
	p := endpointPicker{network: "testnet", override: []string{"a", "b", "c"}}
	if p.current() != "a" {
		t.Fatalf("primary must be used first, got %s", p.current())
	}

	p.failed("b") // 非当前节点失败不切换
	if p.current() != "a" {
		t.Fatalf("failure of a non-current node must not move the cursor, got %s", p.current())
	}

	p.failed("a")
	if p.current() != "b" {
		t.Fatalf("failure of the current node must move to the next, got %s", p.current())
	}

	p.failed("a") // 并发 worker 重复上报已切走的节点，不再切换
	if p.current() != "b" {
		t.Fatalf("duplicate failure report must be ignored, got %s", p.current())
	}

	p.failed("b")
	p.failed("c")
	if p.current() != "a" {
		t.Fatalf("cursor must wrap around, got %s", p.current())
	}

	single := endpointPicker{network: "testnet", override: []string{"only"}}
	single.failed("only")
	if single.current() != "only" {
		t.Fatal("single endpoint must stay in use")
	}

	empty := endpointPicker{network: "no-such-network"}
	if empty.current() != "" {
		t.Fatalf("unknown network must yield empty endpoint, got %q", empty.current())
	}
	empty.failed("") // 不得 panic
}

func TestTakeJobPrefersRealtimeQueue(t *testing.T) {
	ctx := context.Background()
	realtime := chanx.NewUnboundedChan[int](ctx, 4)
	lookback := chanx.NewUnboundedChan[int](ctx, 4)

	lookback.In <- 100
	time.Sleep(20 * time.Millisecond)
	realtime.In <- 1
	time.Sleep(20 * time.Millisecond)

	if j, ok := takeJob(ctx, realtime, lookback); !ok || j != 1 {
		t.Fatalf("realtime job must be taken first, got %d ok=%v", j, ok)
	}
	if j, ok := takeJob(ctx, realtime, lookback); !ok || j != 100 {
		t.Fatalf("lookback job must be taken once realtime is empty, got %d ok=%v", j, ok)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, ok := takeJob(cancelled, realtime, lookback); ok {
		t.Fatal("cancelled context must stop the dispatcher")
	}
}

func TestReplayValidatesRangeAndDispatches(t *testing.T) {
	var got []int64
	registerScanner("testnet", func() ScanStatus {
		return ScanStatus{Network: "testnet", HeadHeight: 42, Endpoint: "https://x.example"}
	}, func(from, to, jobID int64) int {
		got = append(got, from, to)

		return int(to - from + 1)
	})

	n, err := Replay("testnet", 10, 12)
	if err != nil || n != 3 || len(got) != 2 || got[0] != 10 || got[1] != 12 {
		t.Fatalf("replay should dispatch the range, n=%d err=%v got=%v", n, err, got)
	}
	jobs := model.RecentScanJobs("testnet", 1)
	if len(jobs) != 1 || jobs[0].Kind != model.ScanJobKindReplay || jobs[0].Status != model.ScanJobStatusRunning || jobs[0].FromHeight != 10 || jobs[0].ToHeight != 12 {
		t.Fatalf("manual replay must be recorded as a running replay job, got %+v", jobs)
	}

	for _, tc := range []struct {
		network  string
		from, to int64
	}{
		{"unknown", 1, 1},
		{"testnet", 0, 1},
		{"testnet", 5, 4},
		{"testnet", 1, replayMaxBlocks + 1},
	} {
		if _, err := Replay(tc.network, tc.from, tc.to); err == nil {
			t.Fatalf("Replay(%s, %d, %d) must be rejected", tc.network, tc.from, tc.to)
		}
	}

	var found *ScanStatus
	for _, st := range Statuses() {
		if st.Network == "testnet" {
			found = &st
		}
	}
	if found == nil || found.HeadHeight != 42 || found.SuccessRate == "" || found.Endpoint != "https://x.example" {
		t.Fatalf("Statuses must include the registered scanner with common fields filled, got %+v", found)
	}
}

func TestScanAlertIsRateLimited(t *testing.T) {
	alerts := recordAlerts(t)

	scanAlert("unit_test_key", time.Minute, "t", "first")
	scanAlert("unit_test_key", time.Minute, "t", "second")

	if len(*alerts) != 1 || (*alerts)[0].text != "first" {
		t.Fatalf("same key within the window must alert once, got %+v", *alerts)
	}
}

func TestLookbackTrackerMarksDoneOnlyWhenAllBlocksSucceed(t *testing.T) {
	tr := newLookbackTracker()
	orders := []int64{101, 102}

	if !tr.begin("net-a", orders) {
		t.Fatal("begin should succeed")
	}
	if tr.begin("net-a", []int64{103}) {
		t.Fatal("second begin on the same network must be rejected")
	}
	for _, id := range orders {
		if !tr.skip(id) {
			t.Fatalf("order %d should be inflight", id)
		}
	}

	tr.track("net-a", 1)
	tr.track("net-a", 2)
	tr.done("net-a", 1, true)
	tr.sealed("net-a")

	// 还有区块未完成，不得结算
	if !tr.inflight("net-a") {
		t.Fatal("job should still be inflight with pending blocks")
	}

	tr.done("net-a", 2, true)
	if tr.inflight("net-a") {
		t.Fatal("job should be settled")
	}
	for _, id := range orders {
		if v, _ := tr.state.Load(id); v != lookbackDone {
			t.Fatalf("order %d should be done, got %v", id, v)
		}
	}
}

func TestLookbackTrackerReleasesOrdersWhenAnyBlockFails(t *testing.T) {
	tr := newLookbackTracker()
	tr.begin("net-b", []int64{201})
	tr.track("net-b", 10)
	tr.track("net-b", 11)
	tr.sealed("net-b")

	tr.done("net-b", 10, true)
	tr.done("net-b", 11, false)

	if tr.inflight("net-b") {
		t.Fatal("job should be settled")
	}
	if tr.skip(201) {
		t.Fatal("order must return to pending after a block was abandoned")
	}
}

func TestLookbackTrackerAbortReleasesOrders(t *testing.T) {
	tr := newLookbackTracker()
	tr.begin("net-c", []int64{301})
	tr.track("net-c", 5)
	tr.abort("net-c")

	// abort 后仍需等待已入队区块完成，但结果一定是失败
	if !tr.inflight("net-c") {
		t.Fatal("job should wait for tracked blocks")
	}
	tr.done("net-c", 5, true)
	if tr.inflight("net-c") || tr.skip(301) {
		t.Fatal("aborted job must release its orders")
	}

	// 没有任何区块入队时 abort 应立即释放
	tr.begin("net-d", []int64{401})
	tr.abort("net-d")
	if tr.inflight("net-d") || tr.skip(401) {
		t.Fatal("abort with no tracked blocks must release immediately")
	}
}

func TestLookbackTrackerIgnoresUntrackedKeys(t *testing.T) {
	tr := newLookbackTracker()
	tr.begin("net-e", []int64{501})
	tr.track("net-e", 7)
	tr.sealed("net-e")

	tr.done("net-e", 8, false) // 实时扫描的其它区块失败不影响回溯任务
	if !tr.inflight("net-e") || !tr.skip(501) {
		t.Fatal("untracked key must be ignored")
	}
	tr.done("other-net", 7, false)
	if !tr.inflight("net-e") {
		t.Fatal("other network must be ignored")
	}
}

func TestScanRetryDelayIsBoundedWithJitter(t *testing.T) {
	old := scanRetryBaseDelay
	scanRetryBaseDelay = time.Second
	defer func() { scanRetryBaseDelay = old }()

	first := scanRetryDelay(1)
	if first < time.Duration(float64(time.Second)*(1-scanRetryJitter)) || first > time.Duration(float64(time.Second)*(1+scanRetryJitter)) {
		t.Fatalf("attempt 1 delay out of jitter range: %v", first)
	}

	for attempt := 1; attempt <= 40; attempt++ {
		d := scanRetryDelay(attempt)
		if d > time.Duration(float64(scanRetryMaxDelay)*(1+scanRetryJitter)) {
			t.Fatalf("attempt %d delay %v exceeds cap", attempt, d)
		}
	}
}

func TestScanRetryLaterGivesUpAfterMaxAttempts(t *testing.T) {
	q := chanx.NewUnboundedChan[int](context.Background(), 4)

	if _, ok := scanRetryLater(q, 1, scanRetryMaxAttempts); ok {
		t.Fatal("must give up at max attempts")
	}
	if _, ok := scanRetryLater(q, 2, 1); !ok {
		t.Fatal("should schedule a retry below max attempts")
	}

	select {
	case v := <-q.Out:
		if v != 2 {
			t.Fatalf("unexpected job %d", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retry was not re-enqueued")
	}
}
