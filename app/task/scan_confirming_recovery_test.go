package task

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/go-cache"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 独立库确保没有其它测试遗留的新订单、监控钱包或缓存掩盖历史确认缺少链头的问题。
func isolatedHeadRecoveryDB(t *testing.T) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "head-recovery.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	originalDB := model.Db
	oldHead, hadHead := chainBlockNum.Load(conf.Ethereum)
	oldCursor, hadCursor := cursors.Load(conf.Ethereum)
	clearCaches := func() {
		cache.Delete(scanRequiredCacheKey)
		cache.Delete(confirmingCacheKey)
		cache.Delete("mqtt_subscribed_" + conf.Ethereum)
		cache.Delete("wallet_address_set")
	}
	model.Db = db
	chainBlockNum.Delete(conf.Ethereum)
	cursors.Delete(conf.Ethereum)
	clearCaches()
	t.Cleanup(func() {
		model.Db = originalDB
		model.RefreshC()
		clearCaches()
		chainBlockNum.Delete(conf.Ethereum)
		if hadHead {
			chainBlockNum.Store(conf.Ethereum, oldHead)
		}
		cursors.Delete(conf.Ethereum)
		if hadCursor {
			cursors.Store(conf.Ethereum, oldCursor)
		}
		_ = sqlDB.Close()
	})
	if err := model.AutoMigrate(); err != nil {
		t.Fatal(err)
	}
	model.FillDefaultConf()
	model.RefreshC()
}

func TestHistoricalConfirmingOrderRequiresScanOutsideLookbackWindow(t *testing.T) {
	isolatedHeadRecoveryDB(t)
	old := time.Now().Add(-10 * 24 * time.Hour)
	_ = newTestOrder(t, model.UsdtErc20, testPolRecv, "5", model.OrderStatusWaiting, old.Add(-time.Hour), old, "")
	paid := newTestOrder(t, model.UsdtErc20, testPolRecv, "5", model.OrderStatusExpired, old.Add(-time.Hour), old, "")
	if scanRequired(conf.Ethereum) {
		t.Fatal("old waiting and expired orders must retain the existing lookback cutoff")
	}
	if err := paid.MarkConfirming(8000001, testPolSender, testBlockHash(8000002), old.Add(-time.Minute), decimal.NewFromInt(5)); err != nil {
		t.Fatal(err)
	}
	cache.Delete(scanRequiredCacheKey)
	if !scanRequired(conf.Ethereum) {
		t.Fatal("historical confirming order must keep head synchronization active beyond the lookback window")
	}
}

func TestHistoricalConfirmingOrderFetchesHeadAndCompletesOffsetConfirmation(t *testing.T) {
	isolatedHeadRecoveryDB(t)
	model.SetK(model.BlockOffsetConfirm, "1")
	model.RefreshC()
	var callbacks atomic.Int32
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbacks.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(merchant.Close)
	old := time.Now().Add(-10 * 24 * time.Hour)
	paid := newTestOrder(t, model.UsdtErc20, testPolRecv, "5", model.OrderStatusExpired, old.Add(-time.Hour), old, merchant.URL)
	const refBlock = 8000010
	if err := paid.MarkConfirming(refBlock, testPolSender, testBlockHash(refBlock+1), old.Add(-time.Minute), decimal.NewFromInt(5)); err != nil {
		t.Fatal(err)
	}
	cache.Delete(scanRequiredCacheKey)
	cache.Delete(confirmingCacheKey)
	var headRequests, receiptRequests atomic.Int32
	const currentHead = refBlock + 12
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("invalid RPC request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var result any
		switch gjson.ParseBytes(request).Get("method").String() {
		case "eth_getBlockByNumber":
			headRequests.Add(1)
			result = map[string]any{"number": fmt.Sprintf("0x%x", currentHead), "hash": testBlockHash(currentHead), "timestamp": fmt.Sprintf("0x%x", time.Now().Unix()), "transactions": []any{}}
		case "eth_getTransactionReceipt":
			receiptRequests.Add(1)
			result = receiptForOrder(paid)
		default:
			t.Errorf("unexpected RPC method: %s", request)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	t.Cleanup(rpc.Close)
	e := newEvm(conf.Ethereum, block{ConfirmedOffset: 12}, evmNative{}, 0)
	e.rpc.override = []string{rpc.URL}

	// 历史补扫只发现支付，不更新实时链头；确认偏移开启时必须先拿到 head。
	e.tradeConfirmHandle(context.Background())
	if receiptRequests.Load() != 0 {
		t.Fatal("receipt must wait until the confirmation height is known")
	}
	e.syncBlocksForward(context.Background())
	if headRequests.Load() != 1 {
		t.Fatal("historical confirming order must trigger an actual latest-block RPC without monitoring or new orders")
	}
	if head, ok := chainBlockNum.Load(conf.Ethereum); !ok || head.(int64) != currentHead {
		t.Fatalf("historical confirmation head was not synchronized: %v, present=%v", head, ok)
	}
	e.tradeConfirmHandle(context.Background())
	got, _ := model.GetOrderByID(paid.ID)
	if receiptRequests.Load() != 1 || got.Status != model.OrderStatusSuccess {
		t.Fatalf("known head and valid receipt must finish historical confirmation: receipt_requests=%d status=%d", receiptRequests.Load(), got.Status)
	}
	waitOutbox(t, paid.ID, model.NotifyOutboxSent)
	if callbacks.Load() != 1 {
		t.Fatalf("historical confirmation must deliver exactly one merchant callback, got %d", callbacks.Load())
	}
}
