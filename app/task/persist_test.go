package task

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/task/notify"
	"github.com/v03413/go-cache"
)

// ---- 连续游标 ----

func TestCursorAdvancesOnlyContiguouslyAndPersists(t *testing.T) {
	c := cursorOf("cursor-a")
	c.reset(100)
	c.issue(101, 105)

	c.complete(103, 103)
	c.complete(102, 102)
	if c.current() != 100 {
		t.Fatalf("101 still pending, cursor must stay at 100, got %d", c.current())
	}

	c.complete(101, 101)
	if c.current() != 103 {
		t.Fatalf("101-103 done, cursor must be 103, got %d", c.current())
	}

	c.complete(105, 105)
	if c.current() != 103 {
		t.Fatalf("104 still pending, cursor must not skip to 105, got %d", c.current())
	}

	c.complete(200, 200) // 未发出的高度（回溯 / 回放）不影响游标
	c.complete(104, 104)
	if c.current() != 105 || c.pendingCount() != 0 {
		t.Fatalf("all done, cursor must be 105 with nothing pending, got %d / %d", c.current(), c.pendingCount())
	}

	c.flush()
	if h, ok := model.GetScanCursor("cursor-a"); !ok || h != 105 {
		t.Fatalf("cursor must be persisted, got %d ok=%v", h, ok)
	}
}

func TestResumeFromCursorAndGapPolicy(t *testing.T) {
	alerts := recordAlerts(t)

	// 重启，游标在容忍范围内：从游标续扫
	if err := model.SaveScanCursor("resume-a", 1000); err != nil {
		t.Fatal(err)
	}
	if got := resumeFrom("resume-a", 0, 1500, 1000); got != 1000 {
		t.Fatalf("within tolerance must resume from saved cursor, got %d", got)
	}
	if len(model.RecentScanJobs("resume-a", 5)) != 0 {
		t.Fatal("no gap job expected when resuming from cursor")
	}

	// 重启，游标落后太多：记录 gap 任务，从链头开始
	if err := model.SaveScanCursor("resume-b", 1000); err != nil {
		t.Fatal(err)
	}
	if got := resumeFrom("resume-b", 0, 5000, 1000); got != 4999 {
		t.Fatalf("beyond tolerance must align to head-1, got %d", got)
	}
	jobs := model.RecentScanJobs("resume-b", 5)
	if len(jobs) != 1 || jobs[0].Kind != model.ScanJobKindGap || jobs[0].Status != model.ScanJobStatusDeferred || jobs[0].FromHeight != 1001 || jobs[0].ToHeight != 4999 {
		t.Fatalf("skipped range must be recorded as a deferred gap job, got %+v", jobs)
	}
	if len(*alerts) == 0 || (*alerts)[len(*alerts)-1].title != "扫描区间跳过" {
		t.Fatalf("gap must raise an alert, got %+v", *alerts)
	}

	// 运行中链头跳跃：同样记录 gap
	if got := resumeFrom("resume-c", 100, 5000, 1000); got != 4999 {
		t.Fatalf("head jump must align to head-1, got %d", got)
	}
	if jobs := model.RecentScanJobs("resume-c", 5); len(jobs) != 1 || jobs[0].FromHeight != 101 || jobs[0].ToHeight != 4999 {
		t.Fatalf("head jump range must be recorded, got %+v", jobs)
	}

	// 正常前进：不变
	if got := resumeFrom("resume-d", 100, 600, 1000); got != 100 {
		t.Fatalf("normal progress must keep last, got %d", got)
	}
	if len(model.RecentScanJobs("resume-d", 5)) != 0 {
		t.Fatal("no job expected for normal progress")
	}

	// 首次启动、无游标：从链头开始，不记录 gap
	if got := resumeFrom("resume-e", 0, 777, 1000); got != 776 {
		t.Fatalf("fresh start must begin at head-1, got %d", got)
	}
	if len(model.RecentScanJobs("resume-e", 5)) != 0 {
		t.Fatal("fresh start must not record a gap")
	}
}

// ---- 失败任务持久化与重试 ----

type fakeScanner struct {
	calls  []string
	parts  int
	status ScanStatus
}

func (f *fakeScanner) replay(from, to, jobID int64) int {
	f.calls = append(f.calls, fmt.Sprintf("%d-%d#%d", from, to, jobID))

	return f.parts
}

func TestAbandonPersistsJobAndRetryTaskRedispatches(t *testing.T) {
	const net = "jobnet-a"
	fs := &fakeScanner{parts: 1, status: ScanStatus{Network: net}}
	registerScanner(net, func() ScanStatus { return fs.status }, fs.replay)
	entry := scanLogger(net, "https://x", "test", nil)

	scanAbandon(net, 50, 52, 0, "boom", entry)

	jobs := model.RecentScanJobs(net, 5)
	if len(jobs) != 1 || jobs[0].Kind != model.ScanJobKindAbandoned || jobs[0].Status != model.ScanJobStatusPending || jobs[0].FromHeight != 50 || jobs[0].ToHeight != 52 || jobs[0].LastError != "boom" {
		t.Fatalf("abandoned range must be persisted as a pending job, got %+v", jobs)
	}
	if !jobs[0].NextRetryAt.After(time.Now().Add(4 * time.Minute)) {
		t.Fatalf("first retry must be scheduled ~5 minutes later, got %v", jobs[0].NextRetryAt)
	}

	// 未到期：不派发
	scanJobRetry(context.Background())
	if len(fs.calls) != 0 {
		t.Fatalf("job must not be dispatched before next_retry_at, calls=%v", fs.calls)
	}

	// 到期：派发到扫描器并标记 running
	model.Db.Model(&jobs[0]).Update("next_retry_at", time.Now().Add(-time.Second))
	scanJobRetry(context.Background())
	if len(fs.calls) != 1 || fs.calls[0] != fmt.Sprintf("50-52#%d", jobs[0].ID) {
		t.Fatalf("due job must be dispatched with its id, calls=%v", fs.calls)
	}
	if j, _ := model.GetScanJob(jobs[0].ID); j.Status != model.ScanJobStatusRunning {
		t.Fatalf("dispatched job must be running, got %s", j.Status)
	}

	// 队列拥堵：不派发
	model.Db.Model(&jobs[0]).Updates(map[string]any{"status": model.ScanJobStatusPending, "next_retry_at": time.Now().Add(-time.Second)})
	fs.status.LookbackQueue = blockQueueLimit
	scanJobRetry(context.Background())
	if len(fs.calls) != 1 {
		t.Fatalf("job must not be dispatched while lookback queue is congested, calls=%v", fs.calls)
	}
	fs.status.LookbackQueue = 0
	scanJobRetry(context.Background())
	if len(fs.calls) != 2 {
		t.Fatalf("job must be dispatched once the queue has room, calls=%v", fs.calls)
	}

	// 本次成功：done
	scanJobPartDone(jobs[0].ID)
	if j, _ := model.GetScanJob(jobs[0].ID); j.Status != model.ScanJobStatusDone {
		t.Fatalf("job must be done after its only part succeeded, got %s", j.Status)
	}
}

func TestJobFailureBacksOffAndAbandonOfJobBlockDoesNotDuplicate(t *testing.T) {
	const net = "jobnet-b"
	fs := &fakeScanner{parts: 2}
	registerScanner(net, func() ScanStatus { return ScanStatus{Network: net} }, fs.replay)

	job, err := model.CreateScanJob(net, 10, 15, model.ScanJobKindAbandoned, model.ScanJobStatusPending, "x", time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	scanJobRetry(context.Background())
	if len(fs.calls) != 1 {
		t.Fatalf("expected dispatch, calls=%v", fs.calls)
	}

	// 两个批次：一个成功、一个再次放弃 → 任务失败，按任务级退避重排，且不新建任务行
	scanJobPartDone(job.ID)
	scanAbandon(net, 13, 15, job.ID, "still failing", scanLogger(net, "https://x", "test", nil))

	j, _ := model.GetScanJob(job.ID)
	if j.Status != model.ScanJobStatusPending || j.Attempts != 1 || j.LastError != "still failing" || !j.NextRetryAt.After(time.Now().Add(4*time.Minute)) {
		t.Fatalf("failed job must be rescheduled with backoff, got %+v", j)
	}
	if n := len(model.RecentScanJobs(net, 10)); n != 1 {
		t.Fatalf("abandoning a block that belongs to a job must not create another job, got %d rows", n)
	}

	// 重试耗尽 → failed（停止自动重试）+ 告警
	alerts := recordAlerts(t)
	model.Db.Model(&j).Update("attempts", model.ScanJobMaxAttempts-1)
	scanJobBegin(j, 1)
	scanJobPartFailed(j.ID, "dead")
	if j2, _ := model.GetScanJob(job.ID); j2.Status != model.ScanJobStatusFailed {
		t.Fatalf("job must become failed after max attempts, got %s", j2.Status)
	}
	if len(*alerts) != 1 || (*alerts)[0].title != "扫描任务重试耗尽" {
		t.Fatalf("exhausted job must alert once, got %+v", *alerts)
	}
}

func TestResetStaleScanJobsRequeuesRunningJobsAfterRestart(t *testing.T) {
	job, err := model.CreateScanJob("stale-net", 1, 2, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	model.Db.Model(&job).UpdateColumn("updated_at", time.Now().Add(-2*time.Hour))

	if n := model.ResetStaleScanJobs(30 * time.Minute); n != 1 {
		t.Fatalf("expected 1 stale job reset, got %d", n)
	}
	if j, _ := model.GetScanJob(job.ID); j.Status != model.ScanJobStatusPending {
		t.Fatalf("stale running job must be pending again, got %s", j.Status)
	}
}

func TestReplayJobCompletesWhenAllPartsFinish(t *testing.T) {
	const net = "jobnet-c"
	fs := &fakeScanner{parts: 3}
	registerScanner(net, func() ScanStatus { return ScanStatus{Network: net} }, fs.replay)

	if _, err := Replay(net, 1, 9); err != nil {
		t.Fatal(err)
	}
	job := model.RecentScanJobs(net, 1)[0]
	scanJobPartDone(job.ID)
	scanJobPartDone(job.ID)
	if j, _ := model.GetScanJob(job.ID); j.Status != model.ScanJobStatusRunning {
		t.Fatalf("job must stay running until the last part, got %s", j.Status)
	}
	scanJobPartDone(job.ID)
	if j, _ := model.GetScanJob(job.ID); j.Status != model.ScanJobStatusDone {
		t.Fatalf("job must be done after all parts, got %s", j.Status)
	}
}

// ---- 链上流水 / 订单匹配 / 对账 / 回调 outbox ----

var testOrderSeq atomic.Int64

func newTestOrder(t *testing.T, tradeType model.TradeType, address, amount string, status int, createdAt, expiredAt time.Time, notifyURL string) model.Order {
	t.Helper()
	seq := testOrderSeq.Add(1)
	confirmed := createdAt
	order := model.Order{
		OrderId:      fmt.Sprintf("merchant-%d", seq),
		TradeId:      fmt.Sprintf("trade-%d-%d", time.Now().UnixNano(), seq),
		TradeType:    tradeType,
		Fiat:         "CNY",
		Crypto:       "USDT",
		Rate:         "7.00",
		Amount:       amount,
		Money:        "7.00",
		Address:      address,
		MatchAddress: address,
		Status:       status,
		ApiType:      model.OrderApiTypeEpusdt,
		NotifyUrl:    notifyURL,
		ExpiredAt:    expiredAt,
		ConfirmedAt:  &confirmed,
		AutoTimeAt:   model.AutoTimeAt{CreatedAt: (*model.Datetime)(&createdAt), UpdatedAt: (*model.Datetime)(&createdAt)},
	}
	if err := model.Db.Create(&order).Error; err != nil {
		t.Fatal(err)
	}

	return order
}

func ensureWallet(t *testing.T, tradeType model.TradeType, address string) {
	t.Helper()
	var w model.Wallet
	if model.Db.Where("match_addr = ? and trade_type = ?", address, string(tradeType)).Limit(1).Find(&w).RowsAffected == 0 {
		w = model.Wallet{Name: "t", Status: model.WaStatusEnable, Address: address, MatchAddr: address, TradeType: string(tradeType)}
		if err := model.Db.Create(&w).Error; err != nil {
			t.Fatal(err)
		}
	}
	cache.Delete("wallet_address_set")
}

func TestPersistTransfersIsIdempotentAndOnlyForWalletAddresses(t *testing.T) {
	const wallet = "0x9999999999999999999999999999999999999901"
	ensureWallet(t, model.UsdtPolygon, wallet)
	now := time.Now()
	batch := []transfer{
		{Network: conf.Polygon, TxHash: "0xaaa1", Index: 3, Amount: decimal.RequireFromString("1.5"), FromAddress: "0xfrom", RecvAddress: wallet, Timestamp: now, TradeType: model.UsdtPolygon, BlockNum: 10},
		{Network: conf.Polygon, TxHash: "0xaaa1", Index: 7, Amount: decimal.RequireFromString("2.5"), FromAddress: "0xfrom", RecvAddress: wallet, Timestamp: now, TradeType: model.UsdtPolygon, BlockNum: 10},
		{Network: conf.Polygon, TxHash: "0xbbb1", Amount: decimal.RequireFromString("9"), FromAddress: "0xfrom", RecvAddress: "0x0000000000000000000000000000000000000abc", Timestamp: now, TradeType: model.UsdtPolygon, BlockNum: 10},
	}

	for i := 0; i < 10; i++ { // 同一区块重复扫描十次
		persistTransfers(batch)
	}

	var rows []model.ChainTransfer
	model.Db.Where("tx_hash in (?)", []string{"0xaaa1", "0xbbb1"}).Order("event_index asc").Find(&rows)
	if len(rows) != 2 || rows[0].EventIndex != 3 || rows[1].EventIndex != 7 || rows[0].ToAddress != wallet {
		t.Fatalf("expected exactly the two wallet transfers (one per event index) after 10 scans, got %+v", rows)
	}
}

func TestLatePaymentRecoveryMatchesExpiredOrderAndMarksLedger(t *testing.T) {
	const addr = "LateRecvOwnerWallet111111111111111111111111"
	ensureWallet(t, model.UsdtSolana, addr)
	now := time.Now()
	order := newTestOrder(t, model.UsdtSolana, addr, "4.478", model.OrderStatusExpired, now.Add(-2*time.Hour), now.Add(-time.Hour), "")

	batch := []transfer{{
		Network: conf.Solana, TxHash: "LateSig1", Index: 0, Amount: decimal.RequireFromString("4.478"),
		FromAddress: "payer", RecvAddress: addr, Timestamp: now.Add(-90 * time.Minute), TradeType: model.UsdtSolana, BlockNum: 123,
	}}
	persistTransfers(batch)

	// 实时窗口（payment_lookback_hour=3h）内的过期订单：补扫到过期前的付款应进入确认流程
	other := matchTransfers(batch, getReceivableOrders())
	if len(other) != 0 {
		t.Fatalf("late payment made before expiry must match the expired order, unmatched=%v", other)
	}
	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusConfirming || got.RefHash != "LateSig1" {
		t.Fatalf("order must be confirming with the tx hash, got status=%d hash=%s", got.Status, got.RefHash)
	}
	var row model.ChainTransfer
	model.Db.Where("tx_hash = ?", "LateSig1").Find(&row)
	if row.MatchStatus != model.ChainTransferMatched || row.OrderID != order.ID {
		t.Fatalf("ledger row must be marked matched with the order id, got %+v", row)
	}

	// 付款发生在过期之后：不能匹配
	order2 := newTestOrder(t, model.UsdtSolana, addr, "4.479", model.OrderStatusExpired, now.Add(-2*time.Hour), now.Add(-time.Hour), "")
	late := []transfer{{Network: conf.Solana, TxHash: "LateSig2", Amount: decimal.RequireFromString("4.479"), FromAddress: "payer", RecvAddress: addr, Timestamp: now.Add(-30 * time.Minute), TradeType: model.UsdtSolana, BlockNum: 124}}
	if other := matchTransfers(late, getReceivableOrders()); len(other) != 1 {
		t.Fatal("payment after expiry must not match")
	}
	if got2, _ := model.GetOrderByID(order2.ID); got2.Status != model.OrderStatusExpired {
		t.Fatalf("order paid after expiry must stay expired, got %d", got2.Status)
	}
}

func TestReconcileMatchesPreviouslyUnmatchedTransfer(t *testing.T) {
	const addr = "ReconcileRecvOwner1111111111111111111111111"
	ensureWallet(t, model.UsdtSolana, addr)
	now := time.Now()

	// 先有入账落库（当时没有匹配到订单），订单在对账窗口内
	batch := []transfer{{Network: conf.Solana, TxHash: "ReconSig1", Amount: decimal.RequireFromString("8.881"), FromAddress: "payer", RecvAddress: addr, Timestamp: now.Add(-20 * time.Hour), TradeType: model.UsdtSolana, BlockNum: 5}}
	persistTransfers(batch)
	order := newTestOrder(t, model.UsdtSolana, addr, "8.881", model.OrderStatusExpired, now.Add(-21*time.Hour), now.Add(-19*time.Hour), "")

	alerts := recordAlerts(t)
	reconcileTransfers(context.Background())

	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusConfirming || got.RefHash != "ReconSig1" {
		t.Fatalf("reconciliation must match the order, got status=%d hash=%s", got.Status, got.RefHash)
	}
	var row model.ChainTransfer
	model.Db.Where("tx_hash = ?", "ReconSig1").Find(&row)
	if row.MatchStatus != model.ChainTransferMatched || row.OrderID != order.ID {
		t.Fatalf("reconciled transfer must be marked matched, got %+v", row)
	}
	if len(*alerts) != 1 || (*alerts)[0].title != "对账补认单" {
		t.Fatalf("reconciliation that recovers an order must alert, got %+v", *alerts)
	}
}

func waitOutbox(t *testing.T, orderID int64, want string) model.NotifyOutbox {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		row, ok := model.GetNotifyOutbox(orderID)
		if ok && row.Status == want {
			return row
		}
		if time.Now().After(deadline) {
			t.Fatalf("outbox for order %d did not reach %q in time, got %+v", orderID, want, row)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMarkFinalConfirmedIsIdempotentAndDeliversViaOutbox(t *testing.T) {
	var hits atomic.Int32
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(merchant.Close)

	now := time.Now()
	order := newTestOrder(t, model.UsdtSolana, "ConfirmRecv1", "1.001", model.OrderStatusConfirming, now.Add(-10*time.Minute), now.Add(10*time.Minute), merchant.URL)

	markFinalConfirmed(order)
	markFinalConfirmed(order) // 同一订单重复确认

	var count int64
	model.Db.Model(&model.NotifyOutbox{}).Where("order_id = ?", order.ID).Count(&count)
	if count != 1 {
		t.Fatalf("exactly one outbox row per order, got %d", count)
	}

	row := waitOutbox(t, order.ID, model.NotifyOutboxSent)
	if row.LastHttpStatus != 200 || row.LastResponse != "ok" || row.SentAt == nil {
		t.Fatalf("sent row must carry the merchant response, got %+v", row)
	}
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("merchant must receive exactly one callback, got %d", hits.Load())
	}
	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusSuccess || got.NotifyState != model.OrderNotifyStateSucc {
		t.Fatalf("order must be success with notify_state=1, got %+v", got)
	}

	// 商户已确认过的订单，再次触发不会重复回调
	markFinalConfirmed(order)
	time.Sleep(100 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("already-sent order must not be re-notified, got %d hits", hits.Load())
	}
}

func TestNotifyOutboxRecordsFailureAndRetriesWithBackoff(t *testing.T) {
	var mode atomic.Int32 // 0 → 500, 1 → 200 success
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("merchant down"))

			return
		}
		_, _ = w.Write([]byte("success"))
	}))
	t.Cleanup(merchant.Close)

	now := time.Now()
	order := newTestOrder(t, model.UsdtSolana, "OutboxRecv1", "2.002", model.OrderStatusSuccess, now.Add(-10*time.Minute), now.Add(10*time.Minute), merchant.URL)

	if err := notify.Handle(order); err == nil {
		t.Fatal("500 from merchant must be reported as failure")
	}
	row, ok := model.GetNotifyOutbox(order.ID)
	if !ok || row.Status != model.NotifyOutboxPending || row.Attempts != 1 || row.LastHttpStatus != 500 || !strings.Contains(row.LastResponse, "merchant down") || !row.NextRetryAt.After(time.Now().Add(50*time.Second)) {
		t.Fatalf("failure must be recorded with status/body and a backoff of ~1 minute, got %+v", row)
	}

	// 未到期：重试任务不发送
	notifyRetry(context.Background())
	time.Sleep(50 * time.Millisecond)
	if r, _ := model.GetNotifyOutbox(order.ID); r.Attempts != 1 {
		t.Fatalf("not-due row must not be retried, got %+v", r)
	}

	// 到期后商户恢复：发送成功
	mode.Store(1)
	model.Db.Model(&row).Update("next_retry_at", time.Now().Add(-time.Second))
	notifyRetry(context.Background())
	sent := waitOutbox(t, order.ID, model.NotifyOutboxSent)
	if sent.Attempts != 2 || sent.LastHttpStatus != 200 {
		t.Fatalf("second attempt must succeed, got %+v", sent)
	}

	// 重试耗尽：dead
	order2 := newTestOrder(t, model.UsdtSolana, "OutboxRecv2", "2.003", model.OrderStatusSuccess, now, now.Add(10*time.Minute), merchant.URL)
	mode.Store(0)
	if err := model.EnqueueNotify(model.Db, order2.ID, order2.TradeId); err != nil {
		t.Fatal(err)
	}
	r2, _ := model.GetNotifyOutbox(order2.ID)
	model.Db.Model(&r2).Update("attempts", model.NotifyMaxRetryNum()-1)
	_ = notify.Handle(order2)
	if dead, _ := model.GetNotifyOutbox(order2.ID); dead.Status != model.NotifyOutboxDead {
		t.Fatalf("row must be dead after max attempts, got %+v", dead)
	}
	// dead 的事件不再被重试任务派发
	notifyRetry(context.Background())
	time.Sleep(50 * time.Millisecond)
	if again, _ := model.GetNotifyOutbox(order2.ID); again.Attempts != model.NotifyMaxRetryNum() {
		t.Fatalf("dead row must not be retried, got %+v", again)
	}
}
