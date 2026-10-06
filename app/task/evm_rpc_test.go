package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/go-cache"
	"gorm.io/gorm"
)

type ethereumRPCMock struct {
	fullDenied   bool
	native       bool
	timestamp    int64
	fullRequests int
	headerReqs   int
	logsRequests int
}

func ethereumTransferEvent(n int64) map[string]any {
	return map[string]any{
		"address": conf.UsdtErc20,
		"topics":  []string{evmTransferEvent, "0x000000000000000000000000" + testPolSender[2:], "0x000000000000000000000000" + testPolRecv[2:]},
		"data":    fmt.Sprintf("0x%064x", 5000000), "blockNumber": fmt.Sprintf("0x%x", n),
		"blockHash": testBlockHash(n), "transactionHash": testBlockHash(n + 1), "logIndex": "0x0",
	}
}

func (m *ethereumRPCMock) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	requests := gjson.ParseBytes(body).Array()
	items := make([]map[string]any, 0, len(requests))
	for _, request := range requests {
		n := request.Get("id").Int()
		item := map[string]any{"jsonrpc": "2.0", "id": n}
		switch request.Get("method").String() {
		case "eth_getBlockByNumber":
			full := request.Get("params.1").Bool()
			if full {
				m.fullRequests++
			} else {
				m.headerReqs++
			}
			if full && m.fullDenied {
				item["error"] = map[string]any{"code": -32600, "message": "fullTransactions is not available on this plan"}
				items = append(items, item)
				continue
			}
			transactions := []any{}
			if full && m.native {
				transactions = append(transactions, map[string]any{
					"input": "0x", "value": "0xde0b6b3a7640000", "from": testPolSender, "to": testPolRecv,
					"hash": testBlockHash(n + 2), "transactionIndex": "0x0",
				})
			}
			item["result"] = map[string]any{"number": fmt.Sprintf("0x%x", n), "hash": testBlockHash(n), "timestamp": fmt.Sprintf("0x%x", m.timestamp), "transactions": transactions}
		case "eth_getLogs":
			m.logsRequests++
			item["result"] = []any{ethereumTransferEvent(n)}
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		items = append(items, item)
	}
	_ = json.NewEncoder(w).Encode(items)
}

func newEthereumRPCMock(t *testing.T, mock *ethereumRPCMock) *evm {
	t.Helper()
	if mock.timestamp == 0 {
		mock.timestamp = time.Now().Unix()
	}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	t.Cleanup(server.Close)
	e := newEvm(conf.Ethereum, block{}, evmNative{Parse: true, Decimal: conf.EthereumEthDecimals, TradeType: model.EthereumEth}, 0)
	e.rpc.override = []string{server.URL}
	ensureWallet(t, model.UsdtErc20, testPolRecv)
	return e
}

func assertEthereumLedger(t *testing.T, n int64, tradeType model.TradeType, index int) {
	t.Helper()
	var row model.ChainTransfer
	if err := model.Db.Where("network = ? and block_num = ? and trade_type = ?", conf.Ethereum, n, tradeType).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.EventIndex != index || row.ToAddress != testPolRecv {
		t.Fatalf("unexpected persisted transfer: %+v", row)
	}
}

func TestEvmERC20WorksWhenFullTransactionsIsForbidden(t *testing.T) {
	const n = 700001
	mock := &ethereumRPCMock{fullDenied: true}
	e := newEthereumRPCMock(t, mock)
	e.scanBlocks(evmBlock{From: n, To: n})
	if mock.fullRequests != 0 || mock.headerReqs != 1 || mock.logsRequests != 1 {
		t.Fatalf("token-only demand must never request full transactions: %+v", mock)
	}
	assertEthereumLedger(t, n, model.UsdtErc20, 0)
	transfers, ok := recvTransfers(t, time.Second)
	if !ok || len(transfers) != 1 || transfers[0].TradeType != model.UsdtErc20 || transfers[0].Amount.String() != "5" {
		t.Fatalf("ERC20 payment must be delivered even on a token-only node: %+v", transfers)
	}
	if job, ok := recvBlockRetry(t, e, 10*time.Millisecond); ok {
		t.Fatalf("ERC20 success must not retry: %+v", job)
	}
}

func TestEvmNativeRestrictionPersistsRetryWithoutBlockingERC20(t *testing.T) {
	const n = 700002
	mock := &ethereumRPCMock{fullDenied: true}
	e := newEthereumRPCMock(t, mock)
	order := newTestOrder(t, model.EthereumEth, testPolRecv, "1", model.OrderStatusWaiting,
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour), "")
	t.Cleanup(func() { model.Db.Delete(&order) })
	primary := e.rpc.current()
	backup := httptest.NewServer(http.HandlerFunc(mock.handler))
	t.Cleanup(backup.Close)
	e.rpc.override = []string{primary, backup.URL}
	cursor := cursorOf(conf.Ethereum)
	cursor.reset(n - 1)
	cursor.issue(n, n)
	e.scanBlocks(evmBlock{From: n, To: n})
	assertEthereumLedger(t, n, model.UsdtErc20, 0)
	if transfers, ok := recvTransfers(t, time.Second); !ok || len(transfers) != 1 || transfers[0].TradeType != model.UsdtErc20 {
		t.Fatalf("ERC20 payment must be queued before native recovery: %+v", transfers)
	}
	if cursor.current() != n {
		t.Fatal("token cursor may advance once native retry is durably recorded")
	}
	var jobs []model.ScanJob
	if err := model.Db.Where("network = ? and from_height = ? and kind = ?", conf.Ethereum, n, model.ScanJobKindNative).Find(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.ScanJobStatusPending || !strings.Contains(jobs[0].LastError, "fullTransactions") {
		t.Fatalf("native limitation must leave a durable recovery task: %+v", jobs)
	}
	t.Cleanup(func() { model.Db.Delete(&jobs) })
	if e.rpc.current() != primary || e.nativeEndpoint() != backup.URL {
		t.Fatal("native failure must only rotate native RPC, preserving a healthy token node")
	}
}

func TestEvmNativePaymentsAndHistoricalReplayRemainSupported(t *testing.T) {
	const n = 700003
	mock := &ethereumRPCMock{native: true}
	e := newEthereumRPCMock(t, mock)
	old := time.Now().Add(-30 * 24 * time.Hour)
	order := newTestOrder(t, model.EthereumEth, testPolRecv, "1", model.OrderStatusExpired, old.Add(-time.Hour), old, "")
	t.Cleanup(func() { model.Db.Delete(&order) })
	if e.nativeRequired(evmBlock{From: n, To: n}) {
		t.Fatal("old native orders alone must not require full transactions for live ERC20 scanning")
	}
	job, err := model.CreateScanJob(conf.Ethereum, n, n, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&job) })
	scanJobBegin(job, 1)
	e.scanBlocks(evmBlock{From: n, To: n, Lookback: true, JobID: job.ID})
	assertEthereumLedger(t, n, model.UsdtErc20, 0)
	assertEthereumLedger(t, n, model.EthereumEth, -1)
	for _, typ := range []model.TradeType{model.UsdtErc20, model.EthereumEth} {
		transfers, ok := recvTransfers(t, time.Second)
		if !ok || len(transfers) != 1 || transfers[0].TradeType != typ {
			t.Fatalf("expected %s transfer, got %+v", typ, transfers)
		}
	}
	if mock.fullRequests != 1 {
		t.Fatal("history containing native orders must retain native block parsing")
	}
}

func TestEvmNativeRetryDoesNotRepeatTokenLogs(t *testing.T) {
	const n = 700004
	mock := &ethereumRPCMock{native: true}
	e := newEthereumRPCMock(t, mock)
	job, err := model.CreateScanJob(conf.Ethereum, n, n, model.ScanJobKindNative, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&job) })
	scanJobBegin(job, 1)
	if e.replay(n, n, job.ID) != 1 {
		t.Fatal("native retry must enqueue one batch")
	}
	queued := <-e.lookbackQueue.Out
	if !queued.NativeOnly {
		t.Fatal("native retry kind must survive persistent task dispatch")
	}
	e.scanBlocks(queued)
	if mock.headerReqs != 0 || mock.logsRequests != 0 || mock.fullRequests != 1 {
		t.Fatalf("native retries must fetch only full blocks: %+v", mock)
	}
	transfers, ok := recvTransfers(t, time.Second)
	if !ok || len(transfers) != 1 || transfers[0].TradeType != model.EthereumEth {
		t.Fatalf("native retry must retain ETH payment: %+v", transfers)
	}
}

func TestEvmPersistenceFailureDoesNotAcknowledgeBlock(t *testing.T) {
	const n = 700005
	mock := &ethereumRPCMock{}
	e := newEthereumRPCMock(t, mock)
	cursor := cursorOf(conf.Ethereum)
	cursor.reset(n - 1)
	cursor.issue(n, n)
	const callback = "evm_token_write_failure"
	if err := model.Db.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == (model.ChainTransfer{}).TableName() {
			tx.AddError(errors.New("ledger unavailable"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Callback().Create().Remove(callback) })
	e.scanBlocks(evmBlock{From: n, To: n})
	if cursor.current() != n-1 {
		t.Fatal("failed ledger persistence must not acknowledge the scan cursor")
	}
	if _, ok := recvTransfers(t, 10*time.Millisecond); ok {
		t.Fatal("unpersisted payment must not be dispatched for matching")
	}
	if retry, ok := recvBlockRetry(t, e, time.Second); !ok || retry.Attempt != 1 {
		t.Fatalf("failed persistence must retry the scan: %+v ok=%v", retry, ok)
	}
}

func receiptForOrder(o model.Order) map[string]any {
	event := ethereumTransferEvent(int64(o.RefBlockNum))
	event["transactionHash"] = o.RefHash
	return map[string]any{
		"transactionHash": o.RefHash, "blockNumber": fmt.Sprintf("0x%x", o.RefBlockNum),
		"blockHash": testBlockHash(int64(o.RefBlockNum)), "status": "0x1", "logs": []any{event},
		"from": testPolSender, "to": conf.UsdtErc20,
	}
}

func TestEvmConfirmationRequiresValidReceiptAndMatchingTransfer(t *testing.T) {
	o := model.Order{TradeType: model.UsdtErc20, Address: testPolRecv, FromAddress: testPolSender, Amount: "5", RefBlockNum: 700006, RefHash: testBlockHash(700007)}
	cases := []struct {
		name   string
		status int
		change func(map[string]any)
		body   string
		want   bool
	}{
		{name: "valid receipt", want: true},
		{name: "http 429 with valid looking receipt", status: http.StatusTooManyRequests},
		{name: "invalid json", body: `{"jsonrpc":"2.0","id":1,"result":{"status":"0x1"}} garbage`},
		{name: "rpc error", body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limited"}}`},
		{name: "missing receipt", body: `{"jsonrpc":"2.0","id":1,"result":null}`},
		{name: "missing transaction hash", change: func(r map[string]any) { delete(r, "transactionHash") }},
		{name: "wrong transaction hash", change: func(r map[string]any) { r["transactionHash"] = testBlockHash(111) }},
		{name: "wrong block", change: func(r map[string]any) { r["blockNumber"] = "0x1" }},
		{name: "failed transaction", change: func(r map[string]any) { r["status"] = "0x0" }},
		{name: "no target event", change: func(r map[string]any) { r["logs"] = []any{} }},
		{name: "wrong contract", change: func(r map[string]any) { r["logs"].([]any)[0].(map[string]any)["address"] = conf.UsdcErc20 }},
		{name: "wrong recipient", change: func(r map[string]any) {
			r["logs"].([]any)[0].(map[string]any)["topics"].([]string)[2] = "0x000000000000000000000000" + testPolSender[2:]
		}},
		{name: "wrong amount", change: func(r map[string]any) {
			r["logs"].([]any)[0].(map[string]any)["data"] = fmt.Sprintf("0x%064x", 1000000)
		}},
		{name: "removed event", change: func(r map[string]any) { r["logs"].([]any)[0].(map[string]any)["removed"] = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			receipt := receiptForOrder(o)
			if tc.change != nil {
				tc.change(receipt)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": receipt})
			}))
			defer server.Close()
			e := newEvm(conf.Ethereum, block{}, evmNative{Parse: true, TradeType: model.EthereumEth}, 0)
			got, err := e.confirmReceipt(context.Background(), server.URL, o)
			if got != tc.want {
				t.Fatalf("confirmed=%v, expected %v (err=%v)", got, tc.want, err)
			}
		})
	}
}

func TestEvmNativeTaskWriteFailureKeepsLookbackPending(t *testing.T) {
	const n = 700008
	mock := &ethereumRPCMock{fullDenied: true}
	e := newEthereumRPCMock(t, mock)
	order := newTestOrder(t, model.EthereumEth, testPolRecv, "1", model.OrderStatusWaiting,
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour), "")
	t.Cleanup(func() { model.Db.Delete(&order) })
	lookbackTrack.begin(conf.Ethereum, []int64{order.ID})
	lookbackTrack.track(conf.Ethereum, n)
	lookbackTrack.sealed(conf.Ethereum)
	const callback = "evm_native_task_write_failure"
	if err := model.Db.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == (model.ScanJob{}).TableName() {
			tx.AddError(errors.New("retry task unavailable"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer model.Db.Callback().Create().Remove(callback)
	e.scanBlocks(evmBlock{From: n, To: n, Lookback: true})
	if !lookbackTrack.inflight(conf.Ethereum) {
		t.Fatal("lookback must remain inflight when native retry task cannot be persisted")
	}
	select {
	case retry := <-e.lookbackQueue.Out:
		if retry.NativeOnly || retry.Attempt != 1 {
			t.Fatalf("task-write failure must preserve complete lookback context: %+v", retry)
		}
		if _, ok := recvTransfers(t, time.Second); !ok {
			t.Fatal("token payment must still reach matching during native recovery")
		}
		model.Db.Callback().Create().Remove(callback)
		mock.fullDenied = false
		mock.native = true
		e.scanBlocks(retry)
		for i := 0; i < 2; i++ {
			if _, ok := recvTransfers(t, time.Second); !ok {
				t.Fatal("retry must finish both token and native processing")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("failed native task write must retry the lookback batch")
	}
	if lookbackTrack.inflight(conf.Ethereum) || !lookbackTrack.skip(order.ID) {
		t.Fatal("recovered lookback must settle its original pending key")
	}
}

func TestEvmConfirmationRotatesBadEndpointAndConfirmsOnBackup(t *testing.T) {
	merchant := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(merchant.Close)
	now := time.Now()
	order := newTestOrder(t, model.UsdtErc20, testPolRecv, "5", model.OrderStatusConfirming,
		now.Add(-time.Minute), now.Add(time.Hour), merchant.URL)
	order.FromAddress = testPolSender
	order.RefHash = testBlockHash(700010)
	order.RefBlockNum = 700009
	if err := model.Db.Save(&order).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&order) })
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": receiptForOrder(order)})
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": receiptForOrder(order)})
	}))
	t.Cleanup(good.Close)
	e := newEvm(conf.Ethereum, block{}, evmNative{Parse: true, TradeType: model.EthereumEth}, 0)
	e.rpc.override = []string{bad.URL, good.URL}
	cache.Delete(confirmingCacheKey)
	e.tradeConfirmHandle(context.Background())
	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusConfirming || e.rpc.current() != good.URL {
		t.Fatalf("invalid HTTP receipt must keep confirming and rotate endpoint: status=%d endpoint=%s", got.Status, e.rpc.current())
	}
	e.tradeConfirmHandle(context.Background())
	got, _ = model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusSuccess {
		t.Fatalf("valid matching receipt on backup must finalize the payment, got status %d", got.Status)
	}
	waitOutbox(t, order.ID, model.NotifyOutboxSent)
}

func TestEvmNativeTaskExhaustionDoesNotSettleTokenLookback(t *testing.T) {
	const n = 700011
	e := newEthereumRPCMock(t, &ethereumRPCMock{fullDenied: true})
	job, err := model.CreateScanJob(conf.Ethereum, n, n, model.ScanJobKindNative, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&job); lookbackTrack.abort(conf.Ethereum) })
	scanJobBegin(job, 1)
	lookbackTrack.begin(conf.Ethereum, []int64{900011})
	lookbackTrack.track(conf.Ethereum, n)
	lookbackTrack.sealed(conf.Ethereum)
	e.scanBlocks(evmBlock{From: n, To: n, NativeOnly: true, Lookback: true, JobID: job.ID, Attempt: scanRetryMaxAttempts - 1})
	if !lookbackTrack.inflight(conf.Ethereum) {
		t.Fatal("native task failures must not settle a separate token lookback at the same height")
	}
	got, _ := model.GetScanJob(job.ID)
	if got.Status != model.ScanJobStatusPending || got.Attempts != 1 {
		t.Fatalf("native recovery must continue through its durable task: %+v", got)
	}
}
