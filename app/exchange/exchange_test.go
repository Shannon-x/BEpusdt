package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const (
	testKey    = "k-123"
	testSecret = "s-456"
	testPass   = "p-789"
	testUID    = "88123456"
)

func binanceServer(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	paths := make([]string, 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("X-MBX-APIKEY") != testKey {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"code":-2014,"msg":"API-key format invalid."}`))

			return
		}

		// 校验签名：去掉 signature 后的原始 query 重新计算
		q := r.URL.Query()
		sig := q.Get("signature")
		q.Del("signature")
		mac := hmac.New(sha256.New, []byte(testSecret))
		mac.Write([]byte(q.Encode()))
		if sig != hex.EncodeToString(mac.Sum(nil)) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"code":-1022,"msg":"Signature for this request is not valid."}`))

			return
		}
		if q.Get("recvWindow") == "" || q.Get("timestamp") == "" {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"code":-1102,"msg":"missing timestamp"}`))

			return
		}

		switch r.URL.Path {
		case "/api/v3/account":
			_, _ = w.Write([]byte(`{"uid":88123456,"balances":[]}`))
		case "/sapi/v1/pay/transactions":
			_, _ = w.Write([]byte(`{"code":"000000","message":"success","success":true,"data":[
				{"orderType":"C2C","transactionId":"P1","transactionTime":1700000000000,"amount":"4.478","currency":"USDT","payerInfo":{"binanceId":"555"},"receiverInfo":{"binanceId":"88123456"},"fundsDetail":[{"currency":"USDT","amount":"4.478"}]},
				{"orderType":"C2C","transactionId":"P2","transactionTime":1700000100000,"amount":"9","currency":"USDT","payerInfo":{"binanceId":"88123456"},"receiverInfo":{"binanceId":"777"}},
				{"orderType":"C2C","transactionId":"P3","transactionTime":1700000200000,"amount":"1","currency":"BTC","payerInfo":{"binanceId":"1"},"receiverInfo":{"binanceId":"88123456"},"fundsDetail":[{"currency":"BTC","amount":"1"},{"currency":"USDC","amount":"12.5"}]}
			]}`))
		case "/sapi/v1/capital/deposit/hisrec":
			_, _ = w.Write([]byte(`[
				{"id":"D1","amount":"20.000000","coin":"USDT","network":"BSC","status":1,"transferType":1,"txId":"Internal transfer 123","insertTime":1700000300000},
				{"id":"D2","amount":"30","coin":"USDT","network":"TRX","status":1,"transferType":0,"txId":"abc","insertTime":1700000400000},
				{"id":"D3","amount":"40","coin":"USDC","network":"BSC","status":0,"transferType":1,"insertTime":1700000500000}
			]`))
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)

	return srv, &paths
}

func TestBinanceVerifyAndReceipts(t *testing.T) {
	srv, _ := binanceServer(t)
	c, err := New("binance", []string{srv.URL}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	cred := Credential{ApiKey: testKey, ApiSecret: testSecret}

	uid, err := c.VerifyUID(context.Background(), cred)
	if err != nil || uid != testUID {
		t.Fatalf("verify: uid=%q err=%v", uid, err)
	}

	rs, err := c.Receipts(context.Background(), cred, testUID, time.Unix(1699999000, 0), time.Unix(1700001000, 0))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rs {
		got[r.ID] = r.Crypto + " " + r.Amount.String() + " " + r.Kind
	}
	want := map[string]string{
		"pay:P1":     "USDT 4.478 pay",           // 本账户收款
		"pay:P3:1":   "USDC 12.5 pay",            // 多币种明细里只取支持的币种
		"deposit:D1": "USDT 20 internal_deposit", // 内部划转充值
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected receipts: %v", got)
	}
	for id, v := range want {
		if got[id] != v {
			t.Fatalf("receipt %s = %q, want %q (all=%v)", id, got[id], v, got)
		}
	}
	for _, r := range rs {
		if r.ID == "pay:P1" && (r.From != "555" || r.At.Unix() != 1700000000) {
			t.Fatalf("payer / time not parsed: %+v", r)
		}
	}
}

func TestBinanceCredentialErrorIsNotTransient(t *testing.T) {
	srv, _ := binanceServer(t)
	c, _ := New("binance", []string{srv.URL}, srv.Client())

	_, err := c.VerifyUID(context.Background(), Credential{ApiKey: "wrong", ApiSecret: testSecret})
	if err == nil || !IsCredentialError(err) {
		t.Fatalf("expected credential error, got %v", err)
	}
}

func TestFailoverSkipsDeadEndpoint(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	t.Cleanup(dead.Close)
	srv, paths := binanceServer(t)

	c, _ := New("binance", []string{dead.URL, srv.URL}, srv.Client())
	uid, err := c.VerifyUID(context.Background(), Credential{ApiKey: testKey, ApiSecret: testSecret})
	if err != nil || uid != testUID {
		t.Fatalf("failover must reach the healthy endpoint, uid=%q err=%v", uid, err)
	}
	if len(*paths) != 1 {
		t.Fatalf("healthy endpoint should be hit once, got %d", len(*paths))
	}
}

func okxServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts := r.Header.Get("OK-ACCESS-TIMESTAMP")
		if r.Header.Get("OK-ACCESS-KEY") != testKey || r.Header.Get("OK-ACCESS-PASSPHRASE") != testPass || ts == "" {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"code":"50111","msg":"Invalid OK-ACCESS-KEY","data":[]}`))

			return
		}
		mac := hmac.New(sha256.New, []byte(testSecret))
		mac.Write([]byte(ts + "GET" + r.URL.RequestURI()))
		if r.Header.Get("OK-ACCESS-SIGN") != base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"code":"50113","msg":"Invalid Sign","data":[]}`))

			return
		}

		switch r.URL.Path {
		case "/api/v5/account/config":
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"uid":"88123456","level":"Lv1"}]}`))
		case "/api/v5/asset/bills":
			ccy := r.URL.Query().Get("ccy")
			if ccy == "USDT" {
				_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[
					{"billId":"B1","ccy":"USDT","balChg":"4.478","bal":"100","type":"1","ts":"1700000000000"},
					{"billId":"B2","ccy":"USDT","balChg":"-5","bal":"95","type":"2","ts":"1700000100000"},
					{"billId":"B3","ccy":"USDT","balChg":"7.5","bal":"102.5","type":"72","ts":"1700000200000"},
					{"billId":"B4","ccy":"USDT","balChg":"3","bal":"105.5","type":"130","ts":"1700000300000"}
				]}`))

				return
			}
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"billId":"B5","ccy":"USDC","balChg":"1.25","type":"1","ts":"1700000400000"}]}`))
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestOkxVerifyAndReceipts(t *testing.T) {
	srv := okxServer(t)
	c, _ := New("okx", []string{srv.URL}, srv.Client())
	cred := Credential{ApiKey: testKey, ApiSecret: testSecret, Passphrase: testPass}

	uid, err := c.VerifyUID(context.Background(), cred)
	if err != nil || uid != testUID {
		t.Fatalf("verify: uid=%q err=%v", uid, err)
	}

	rs, err := c.Receipts(context.Background(), cred, testUID, time.Unix(1699999000, 0), time.Unix(1700001000, 0))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rs {
		got[r.ID] = r.Crypto + " " + r.Amount.String()
	}
	want := map[string]string{"bill:B1": "USDT 4.478", "bill:B3": "USDT 7.5", "bill:B5": "USDC 1.25"}
	if len(got) != len(want) {
		t.Fatalf("unexpected receipts (withdrawals / other types must be skipped): %v", got)
	}
	for id, v := range want {
		if got[id] != v {
			t.Fatalf("receipt %s = %q want %q", id, got[id], v)
		}
	}

	_, err = c.VerifyUID(context.Background(), Credential{ApiKey: testKey, ApiSecret: "bad", Passphrase: testPass})
	if err == nil || !IsCredentialError(err) || !strings.Contains(err.Error(), "50113") {
		t.Fatalf("bad signature must be a credential error, got %v", err)
	}
}

func TestNewRejectsUnknownAndEmpty(t *testing.T) {
	if _, err := New("huobi", []string{"https://x"}, nil); err == nil {
		t.Fatal("unknown exchange must be rejected")
	}
	if _, err := New("binance", nil, nil); err == nil {
		t.Fatal("empty endpoints must be rejected")
	}
	_ = url.Values{}
}
