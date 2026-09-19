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

	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/go-cache"
)

const testBinanceUID = "88123456"

// binanceMock 最小化的币安接口：账户 uid、Pay 收款记录、充值记录
func binanceMock(t *testing.T, payAmount string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-MBX-APIKEY") == "" || r.URL.Query().Get("signature") == "" {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"code":-2014,"msg":"API-key format invalid."}`))

			return
		}
		switch r.URL.Path {
		case "/api/v3/account":
			_, _ = w.Write([]byte(`{"uid":88123456}`))
		case "/sapi/v1/pay/transactions":
			_, _ = w.Write([]byte(fmt.Sprintf(`{"code":"000000","message":"success","success":true,"data":[{"orderType":"C2C","transactionId":"PAY1","transactionTime":%d,"amount":"%s","currency":"USDT","payerInfo":{"binanceId":"555"},"receiverInfo":{"binanceId":"%s"}}]}`,
				time.Now().Add(-time.Minute).UnixMilli(), payAmount, testBinanceUID)))
		case "/sapi/v1/capital/deposit/hisrec":
			_, _ = w.Write([]byte(`[]`))
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)

	return srv, &calls
}

func TestWalletExchangeCredentialsRoundTripEncrypted(t *testing.T) {
	w := model.Wallet{Name: "bn", Status: model.WaStatusEnable, Address: testBinanceUID, MatchAddr: testBinanceUID, TradeType: string(model.UsdtBinance)}
	if err := w.Validate(); err != nil {
		t.Fatalf("numeric UID must validate: %v", err)
	}
	if err := w.SetCredentials(model.ExchangeCredential{ApiKey: "k1", ApiSecret: "s1"}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(w.Credentials, "enc:v1:") || strings.Contains(w.Credentials, "s1") {
		t.Fatalf("credentials must be stored encrypted, got %q", w.Credentials)
	}
	if err := model.Db.Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&w) })

	var loaded model.Wallet
	model.Db.Where("id = ?", w.ID).Find(&loaded)
	cred, ok := loaded.GetCredentials()
	if !ok || cred.ApiKey != "k1" || cred.ApiSecret != "s1" || !loaded.HasCredentials {
		t.Fatalf("credentials must round-trip, got %+v ok=%v has=%v", cred, ok, loaded.HasCredentials)
	}

	bad := model.Wallet{Address: "not-a-uid", TradeType: string(model.UsdtOkx)}
	if err := bad.Validate(); err == nil {
		t.Fatal("non-numeric UID must be rejected")
	}
}

func TestExchangePollQueuesReceiptsOnceAndConfirmsOrder(t *testing.T) {
	srv, calls := binanceMock(t, "6.607")
	// SetK 在事务提交前刷新缓存，运行时靠 3 秒周期刷新兜底；测试里显式刷新
	model.SetK(model.RpcEndpointBinance, srv.URL)
	model.RefreshC()
	t.Cleanup(func() { model.SetK(model.RpcEndpointBinance, "https://api.binance.com"); model.RefreshC() })

	wallet := model.Wallet{Name: "bn", Status: model.WaStatusEnable, Address: testBinanceUID, MatchAddr: testBinanceUID, TradeType: string(model.UsdtBinance)}
	if err := wallet.SetCredentials(model.ExchangeCredential{ApiKey: "k", ApiSecret: "s"}); err != nil {
		t.Fatal(err)
	}
	if err := model.Db.Create(&wallet).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&wallet) })
	cache.Delete("wallet_address_set")

	s := newExchangeScanner(conf.Binance)

	// 没有待支付订单：不请求交易所 API
	cache.Delete(scanRequiredCacheKey)
	s.poll(context.Background())
	if calls.Load() != 0 {
		t.Fatalf("idle exchange must not be polled, got %d calls", calls.Load())
	}

	now := time.Now()
	order := newTestOrder(t, model.UsdtBinance, testBinanceUID, "6.607", model.OrderStatusWaiting, now.Add(-10*time.Minute), now.Add(20*time.Minute), "")
	cache.Delete(scanRequiredCacheKey)

	s.poll(context.Background())
	transfers, ok := recvTransfers(t, 2*time.Second)
	if !ok || len(transfers) != 1 {
		t.Fatalf("expected one exchange receipt, got %v ok=%v", transfers, ok)
	}
	tr := transfers[0]
	if tr.Network != conf.Binance || tr.TradeType != model.UsdtBinance || tr.RecvAddress != testBinanceUID || tr.Amount.String() != "6.607" || tr.TxHash != "binance:pay:PAY1" || tr.FromAddress != "555" {
		t.Fatalf("unexpected transfer: %+v", tr)
	}
	if got := conf.GetStats()[conf.Binance]; got.Block == "" {
		t.Fatal("successful poll must record success")
	}
	if cursorOf(conf.Binance).current() <= 0 {
		t.Fatal("poll cursor must advance to the poll time")
	}

	// 再次轮询：同一笔入账不重复投递
	s.poll(context.Background())
	if _, again := recvTransfers(t, 300*time.Millisecond); again {
		t.Fatal("already seen receipt must not be queued twice")
	}

	// 入账进入常规匹配：订单进入确认中，交易所入账即终态 → 确认任务直接标记成功并登记回调
	persistTransfers(transfers)
	if other := matchTransfers(transfers, getReceivableOrders()); len(other) != 0 {
		t.Fatalf("receipt must match the pending exchange order, unmatched=%v", other)
	}
	cache.Delete(confirmingCacheKey)
	s.confirm(context.Background())

	got, _ := model.GetOrderByID(order.ID)
	if got.Status != model.OrderStatusSuccess || got.RefHash != "binance:pay:PAY1" {
		t.Fatalf("exchange order must be success after confirm, got status=%d hash=%s", got.Status, got.RefHash)
	}
	if _, ok := model.GetNotifyOutbox(order.ID); !ok {
		t.Fatal("success must enqueue a callback")
	}

	// 回放：忽略去重，重新投递该时间段的入账
	s.replay(now.Add(-time.Hour).Unix(), now.Add(time.Minute).Unix(), 0)
	if _, ok := recvTransfers(t, 2*time.Second); !ok {
		t.Fatal("replay must re-queue receipts in the window")
	}
}

func TestExchangeCredentialErrorAlertsAndDoesNotCountSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"code":"50111","msg":"Invalid OK-ACCESS-KEY"}`))
	}))
	t.Cleanup(srv.Close)
	model.SetK(model.RpcEndpointOkx, srv.URL)
	model.RefreshC()
	t.Cleanup(func() { model.SetK(model.RpcEndpointOkx, "https://www.okx.com"); model.RefreshC() })

	wallet := model.Wallet{Name: "okx", Status: model.WaStatusEnable, Address: "77123456", MatchAddr: "77123456", TradeType: string(model.UsdtOkx)}
	if err := wallet.SetCredentials(model.ExchangeCredential{ApiKey: "k", ApiSecret: "s", Passphrase: "p"}); err != nil {
		t.Fatal(err)
	}
	if err := model.Db.Create(&wallet).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Delete(&wallet) })
	now := time.Now()
	newTestOrder(t, model.UsdtOkx, "77123456", "1.11", model.OrderStatusWaiting, now, now.Add(20*time.Minute), "")
	cache.Delete(scanRequiredCacheKey)
	alerts := recordAlerts(t)

	s := newExchangeScanner(conf.Okx)
	before := cursorOf(conf.Okx).current()
	s.poll(context.Background())

	if len(*alerts) != 1 || (*alerts)[0].title != "交易所 API 凭证无效" {
		t.Fatalf("invalid credentials must alert once, got %+v", *alerts)
	}
	if cursorOf(conf.Okx).current() != before {
		t.Fatal("failed poll must not advance the cursor")
	}
	if _, ok := recvTransfers(t, 200*time.Millisecond); ok {
		t.Fatal("no transfers expected on failure")
	}
}

func TestSeenSetDedupesAndBounds(t *testing.T) {
	set := newSeenSet(time.Hour, 3)
	if !set.add("a") || set.add("a") {
		t.Fatal("first add true, second false")
	}
	set.add("b")
	set.add("c")
	// 超过容量且没有过期项：整体清空后继续工作（下游幂等）
	if !set.add("d") || !set.add("a") {
		t.Fatal("set must keep working after reaching capacity")
	}
}
