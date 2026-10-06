package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/go-cache"
	"gorm.io/gorm"
)

func recoveryTransfer(address, hash, amount string, at time.Time) transfer {
	return transfer{Network: conf.Ethereum, TxHash: hash, Index: 0, Amount: decimal.RequireFromString(amount), FromAddress: "payer", RecvAddress: address, Timestamp: at, TradeType: model.UsdtErc20, BlockNum: 1234}
}

func TestHistoricalReplayMatchesOrderAtPaymentTime(t *testing.T) {
	const addr = "0x5555555555555555555555555555555555555501"
	ensureWallet(t, model.UsdtErc20, addr)
	paidAt := time.Now().Add(-5 * 24 * time.Hour).Truncate(time.Second)
	order := newTestOrder(t, model.UsdtErc20, addr, "12.3400", model.OrderStatusExpired, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	wrongAmount := newTestOrder(t, model.UsdtErc20, addr, "12.35", model.OrderStatusExpired, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	wrongCurrency := newTestOrder(t, model.UsdcErc20, addr, "12.34", model.OrderStatusExpired, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	expiredBeforePayment := newTestOrder(t, model.UsdtErc20, addr, "12.34", model.OrderStatusExpired, paidAt.Add(-2*time.Minute), paidAt.Add(-time.Minute), "")

	batch := []transfer{recoveryTransfer(addr, "historical-five-days", "12.34", paidAt)}
	other, err := processTransferBatch(batch)
	if err != nil || len(other) != 0 {
		t.Fatalf("historical payment must match: other=%v err=%v", other, err)
	}
	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusConfirming || got.RefHash != batch[0].TxHash {
		t.Fatalf("historical order must be confirming: %+v", got)
	}
	for _, candidate := range []model.Order{wrongAmount, wrongCurrency, expiredBeforePayment} {
		got, _ := model.GetOrderByID(candidate.ID)
		if got.Status != model.OrderStatusExpired {
			t.Fatalf("mismatched order %d must stay expired, got %d", candidate.ID, got.Status)
		}
	}
}

func TestReconcileRecoversRecentlyInsertedOldTransferPastFirstPage(t *testing.T) {
	cache.Delete("scan_alert_reconcile_matched")
	t.Cleanup(func() { cache.Delete("scan_alert_reconcile_matched") })
	const addr = "0x5555555555555555555555555555555555555502"
	ensureWallet(t, model.UsdtErc20, addr)
	paidAt := time.Now().Add(-5 * 24 * time.Hour).Truncate(time.Second)
	order := newTestOrder(t, model.UsdtErc20, addr, "14.5", model.OrderStatusExpired, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	oldOrder := newTestOrder(t, model.UsdtErc20, addr, "14.6", model.OrderStatusExpired, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	rows := make([]model.ChainTransfer, 500)
	for i := range rows {
		rows[i] = model.ChainTransfer{Network: conf.Ethereum, TxHash: fmt.Sprintf("recovery-page-filler-%d", i), ToAddress: "no-order-address", TradeType: model.UsdtErc20, Amount: "1", BlockTime: paidAt, MatchStatus: model.ChainTransferUnmatched}
	}
	if err := model.SaveChainTransfers(rows); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Where("tx_hash like ?", "recovery-page-filler-%").Delete(&model.ChainTransfer{}) })
	tr := recoveryTransfer(addr, "old-receipt-new-ingestion", "14.5", paidAt)
	if err := persistTransfers([]transfer{tr}); err != nil {
		t.Fatal(err)
	}
	previouslyIngested := recoveryTransfer(addr, "old-receipt-old-ingestion-replayed", "14.6", paidAt)
	if err := persistTransfers([]transfer{previouslyIngested}); err != nil {
		t.Fatal(err)
	}
	if err := model.Db.Model(&model.ChainTransfer{}).Where("tx_hash = ?", previouslyIngested.TxHash).
		UpdateColumns(map[string]any{"created_at": paidAt, "updated_at": paidAt}).Error; err != nil {
		t.Fatal(err)
	}
	// 之前已经入库的历史未匹配流水再次补扫，只刷新补录时间，随后也能由对账补认。
	if err := persistTransfers([]transfer{previouslyIngested}); err != nil {
		t.Fatal(err)
	}
	// 模拟历史补扫已落库，认单前进程退出；链上时间早于48小时但入库时间在窗口内。
	recordAlerts(t)
	reconcileTransfers(context.Background())
	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusConfirming || got.RefHash != tr.TxHash {
		t.Fatalf("newly ingested old payment after >500 unmatched rows must recover: %+v", got)
	}
	oldGot, _ := model.GetOrderByID(oldOrder.ID)
	if oldGot.Status != model.OrderStatusConfirming || oldGot.RefHash != previouslyIngested.TxHash {
		t.Fatalf("replayed old unmatched ledger must reenter reconciliation window: %+v", oldGot)
	}
}

func TestDuplicateEventZeroCannotMatchSecondOrderOrNotifyAsNonOrder(t *testing.T) {
	const addr = "0x5555555555555555555555555555555555555503"
	ensureWallet(t, model.UsdtErc20, addr)
	paidAt := time.Now().Truncate(time.Second)
	first := newTestOrder(t, model.UsdtErc20, addr, "15", model.OrderStatusWaiting, paidAt.Add(-2*time.Minute), paidAt.Add(time.Minute), "")
	second := newTestOrder(t, model.UsdtErc20, addr, "15", model.OrderStatusWaiting, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	tr := recoveryTransfer(addr, "duplicate-zero-event", "15", paidAt)
	for _, batch := range [][]transfer{{tr, tr}, {tr}, {tr, tr}} {
		other, err := processTransferBatch(batch)
		if err != nil || len(other) != 0 {
			t.Fatalf("matched duplicates must be consumed silently: other=%v err=%v", other, err)
		}
	}
	a, _ := model.GetOrderByID(first.ID)
	b, _ := model.GetOrderByID(second.ID)
	if a.Status != model.OrderStatusConfirming || b.Status != model.OrderStatusWaiting {
		t.Fatalf("one payment must confirm only first order: %d / %d", a.Status, b.Status)
	}
	var count int64
	model.Db.Model(&model.ChainTransfer{}).Where("tx_hash = ?", tr.TxHash).Count(&count)
	if count != 1 {
		t.Fatalf("event index zero must stay zero during duplicate insertion, got %d rows", count)
	}
}

func TestOrderAndLedgerClaimRollBackTogether(t *testing.T) {
	const addr = "0x5555555555555555555555555555555555555504"
	ensureWallet(t, model.UsdtErc20, addr)
	paidAt := time.Now().Truncate(time.Second)
	order := newTestOrder(t, model.UsdtErc20, addr, "16", model.OrderStatusWaiting, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	tr := recoveryTransfer(addr, "atomic-rollback", "16", paidAt)
	if err := persistTransfers([]transfer{tr}); err != nil {
		t.Fatal(err)
	}
	const callback = "recovery_fail_order_update"
	if err := model.Db.Callback().Update().Before("gorm:update").Register(callback, func(db *gorm.DB) {
		if db.Statement.Table == "bep_order" {
			db.AddError(errors.New("injected order write failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, _, err := matchTransfersWithCount([]transfer{tr}, getReceivableOrders())
	model.Db.Callback().Update().Remove(callback)
	if err == nil {
		t.Fatal("injected order update error must propagate")
	}
	got, _ := model.GetOrderByID(order.ID)
	var ledger model.ChainTransfer
	model.Db.Where("tx_hash = ?", tr.TxHash).First(&ledger)
	if got.Status != model.OrderStatusWaiting || got.RefHash != "" || ledger.MatchStatus != model.ChainTransferUnmatched || ledger.OrderID != 0 {
		t.Fatalf("failed transaction must roll back both claims: order=%+v ledger=%+v", got, ledger)
	}
	other, err := processTransferBatch([]transfer{tr})
	if err != nil || len(other) != 0 {
		t.Fatalf("after database recovery the same receipt must be matched: %v %v", other, err)
	}
}

func TestConcurrentClaimAndStaleExpirationCannotOverwritePayment(t *testing.T) {
	const addr = "0x5555555555555555555555555555555555555505"
	ensureWallet(t, model.UsdtErc20, addr)
	paidAt := time.Now().Truncate(time.Second)
	first := newTestOrder(t, model.UsdtErc20, addr, "17", model.OrderStatusWaiting, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	second := newTestOrder(t, model.UsdtErc20, addr, "17", model.OrderStatusWaiting, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	tr := recoveryTransfer(addr, "concurrent-single-claim", "17", paidAt)
	if err := persistTransfers([]transfer{tr}); err != nil {
		t.Fatal(err)
	}
	var claimed atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			o := first
			if i%2 != 0 {
				o = second
			}
			ok, err := model.MatchChainTransfer(&o, tr.Network, tr.TxHash, tr.Index, tr.BlockNum, tr.FromAddress, tr.RecvAddress, tr.Timestamp, tr.Amount)
			if err != nil && !errors.Is(err, model.ErrChainTransferAlreadyMatched) {
				t.Errorf("concurrent claim failed: %v", err)
			}
			if ok {
				claimed.Add(1)
			}
		})
	}
	wg.Wait()
	if claimed.Load() != 1 {
		t.Fatalf("concurrent workers must claim exactly one order, got %d", claimed.Load())
	}
	var ledger model.ChainTransfer
	model.Db.Where("tx_hash = ?", tr.TxHash).First(&ledger)
	stale := first
	if ledger.OrderID == second.ID {
		stale = second
	}
	if stale.SetExpired() {
		t.Fatal("stale waiting snapshot must not expire confirming order")
	}
	if err := stale.SetCanceled(); !errors.Is(err, model.ErrOrderNotReceivable) {
		t.Fatalf("stale waiting snapshot cancellation must reject: %v", err)
	}
	got, _ := model.GetOrderByID(ledger.OrderID)
	if got.Status != model.OrderStatusConfirming || got.RefHash != tr.TxHash {
		t.Fatalf("stale lifecycle update must preserve payment: %+v", got)
	}
}

func TestPersistTransfersReportsWalletQueryAndLedgerWriteFailures(t *testing.T) {
	const addr = "0x5555555555555555555555555555555555555506"
	ensureWallet(t, model.UsdtErc20, addr)
	tr := recoveryTransfer(addr, "persistent-failure-test", "18", time.Now().Truncate(time.Second))
	const queryCallback = "recovery_fail_wallet_query"
	if err := model.Db.Callback().Query().Before("gorm:query").Register(queryCallback, func(db *gorm.DB) {
		if db.Statement.Table == "bep_wallet" {
			db.AddError(errors.New("injected wallet read failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	cache.Delete("wallet_address_set")
	err := persistTransfers([]transfer{tr})
	model.Db.Callback().Query().Remove(queryCallback)
	if err == nil {
		t.Fatal("wallet read error must propagate, not act like empty wallets")
	}
	const createCallback = "recovery_fail_ledger_create"
	if err := model.Db.Callback().Create().Before("gorm:create").Register(createCallback, func(db *gorm.DB) {
		if db.Statement.Table == "bep_chain_transfer" {
			db.AddError(errors.New("injected ledger write failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	err = persistTransfers([]transfer{tr})
	model.Db.Callback().Create().Remove(createCallback)
	if err == nil {
		t.Fatal("ledger write failure must propagate")
	}
	if err := persistTransfers([]transfer{tr}); err != nil {
		t.Fatalf("recovery must allow retry: %v", err)
	}
}

func TestTransferWorkerRetainsFailedBatch(t *testing.T) {
	const addr = "0x5555555555555555555555555555555555555507"
	ensureWallet(t, model.UsdtErc20, addr)
	paidAt := time.Now().Truncate(time.Second)
	order := newTestOrder(t, model.UsdtErc20, addr, "19", model.OrderStatusWaiting, paidAt.Add(-time.Minute), paidAt.Add(time.Minute), "")
	tr := recoveryTransfer(addr, "worker-retained-batch", "19", paidAt)
	var fail atomic.Bool
	fail.Store(true)
	failed := make(chan struct{}, 1)
	const callback = "recovery_worker_fail_once"
	if err := model.Db.Callback().Create().Before("gorm:create").Register(callback, func(db *gorm.DB) {
		if db.Statement.Table == "bep_chain_transfer" && fail.Load() {
			db.AddError(errors.New("injected temporary ledger failure"))
			select {
			case failed <- struct{}{}:
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { orderTransferHandle(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
		model.Db.Callback().Create().Remove(callback)
	})
	transferQueue.In <- []transfer{tr}
	select {
	case <-failed:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not attempt the queued batch")
	}
	fail.Store(false)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := model.GetOrderByID(order.ID)
		if got.Status == model.OrderStatusConfirming {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("worker must retry its retained batch without receiving the transfer again")
}

func TestClassicAmountComparisonUsesDecimalValue(t *testing.T) {
	previous := model.GetK(model.PaymentMatchMode)
	model.SetK(model.PaymentMatchMode, string(model.Classic))
	model.RefreshC()
	t.Cleanup(func() { model.SetK(model.PaymentMatchMode, previous); model.RefreshC() })
	for _, target := range []string{"2", "2.0", "2.0000"} {
		if !amountMatch(decimal.RequireFromString("2.00"), target, string(model.UsdtErc20)) {
			t.Fatalf("equal numeric amount must match %q", target)
		}
	}
	if amountMatch(decimal.RequireFromString("2.0001"), "2.00", string(model.UsdtErc20)) || amountMatch(decimal.RequireFromString("2"), "bad", string(model.UsdtErc20)) {
		t.Fatal("different or invalid amounts must not match")
	}
}

func TestPendingLookbackUsesMaximumOrderEndpoint(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	long := newTestOrder(t, model.UsdcBase, "lookback-base-long", "21", model.OrderStatusExpired, now.Add(-2*time.Hour), now.Add(-10*time.Minute), "")
	short := newTestOrder(t, model.UsdcBase, "lookback-base-short", "22", model.OrderStatusExpired, now.Add(-time.Hour), now.Add(-30*time.Minute), "")
	t.Cleanup(func() { model.Db.Delete(&long); model.Db.Delete(&short) })
	_, endAt, _, ok := pendingLookbackUnix(conf.Base)
	if !ok || endAt != long.ExpiredAt.Unix() {
		t.Fatalf("expired orders endpoint must be max expiry independent of creation order: %d want %d", endAt, long.ExpiredAt.Unix())
	}
	active := newTestOrder(t, model.UsdcBase, "lookback-base-active", "23", model.OrderStatusWaiting, now.Add(-30*time.Minute), now.Add(time.Hour), "")
	t.Cleanup(func() { model.Db.Delete(&active) })
	_, endAt, _, ok = pendingLookbackUnix(conf.Base)
	if !ok || endAt < now.Unix() || endAt > time.Now().Unix() {
		t.Fatalf("active order must extend endpoint to now: %d", endAt)
	}
}
