package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
)

// Binance：
//   - GET /api/v3/account                      校验凭证并取得账户 uid（权重 20）
//   - GET /sapi/v1/pay/transactions            Binance Pay 收款记录，receiverInfo.binanceId == uid 为本账户收款（权重 3000/UID）
//   - GET /sapi/v1/capital/deposit/hisrec      充值记录，transferType=1 为其他币安用户提现到本账户的内部划转（权重 1）
//
// 签名：query 追加 timestamp / recvWindow，signature = hex(HMAC-SHA256(secret, query))，请求头 X-MBX-APIKEY。

const (
	binanceRecvWindow = "5000"
	binancePayLimit   = "100"
)

type binance struct {
	endpoints []string
	client    *http.Client
}

func (b *binance) Name() string { return "binance" }

func (b *binance) VerifyUID(ctx context.Context, cred Credential) (string, error) {
	body, err := b.signedGet(ctx, cred, "/api/v3/account", url.Values{"omitZeroBalances": {"true"}})
	if err != nil {
		return "", err
	}

	uid := gjson.GetBytes(body, "uid")
	if !uid.Exists() || uid.Int() <= 0 {
		return "", &APIError{Message: "账户接口未返回 uid", Credential: true}
	}

	return strconv.FormatInt(uid.Int(), 10), nil
}

func (b *binance) Receipts(ctx context.Context, cred Credential, uid string, since, until time.Time) ([]Receipt, error) {
	window := url.Values{
		"startTime": {strconv.FormatInt(since.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(until.UnixMilli(), 10)},
	}

	receipts := make([]Receipt, 0)

	// Binance Pay 收款
	payQuery := url.Values{"limit": {binancePayLimit}}
	for k, v := range window {
		payQuery[k] = v
	}
	body, err := b.signedGet(ctx, cred, "/sapi/v1/pay/transactions", payQuery)
	if err != nil {
		return nil, err
	}
	res := gjson.ParseBytes(body)
	if res.Get("success").Exists() && !res.Get("success").Bool() {
		return nil, &APIError{Code: res.Get("code").String(), Message: res.Get("message").String()}
	}
	for _, row := range res.Get("data").Array() {
		receipts = append(receipts, binancePayReceipts(row, uid)...)
	}

	// 内部划转充值
	depQuery := url.Values{"limit": {"1000"}}
	for k, v := range window {
		depQuery[k] = v
	}
	body, err = b.signedGet(ctx, cred, "/sapi/v1/capital/deposit/hisrec", depQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range gjson.ParseBytes(body).Array() {
		if row.Get("transferType").Int() != 1 {
			continue
		}
		status := row.Get("status").Int()
		if status != 1 && status != 6 { // 1 成功；6 已到账但暂不可提
			continue
		}
		crypto, ok := supportedCrypto(row.Get("coin").String())
		if !ok {
			continue
		}
		amount, err := decimal.NewFromString(row.Get("amount").String())
		if err != nil || amount.Sign() <= 0 {
			continue
		}

		receipts = append(receipts, Receipt{
			ID:     "deposit:" + row.Get("id").String(),
			Kind:   KindInternalDeposit,
			Crypto: crypto,
			Amount: amount,
			At:     unixTime(row.Get("insertTime").Int()),
		})
	}

	return receipts, nil
}

// binancePayReceipts 一条 Pay 记录可能含多种币种的资金明细，逐个转成入账
func binancePayReceipts(row gjson.Result, uid string) []Receipt {
	if row.Get("receiverInfo.binanceId").String() != uid {
		return nil
	}
	txID := row.Get("transactionId").String()
	if txID == "" {
		return nil
	}
	at := unixTime(row.Get("transactionTime").Int())
	from := row.Get("payerInfo.binanceId").String()
	if from == "" {
		from = row.Get("payerInfo.accountId").String()
	}

	funds := row.Get("fundsDetail").Array()
	if len(funds) == 0 {
		funds = []gjson.Result{row}
	}

	out := make([]Receipt, 0, len(funds))
	for i, fund := range funds {
		crypto, ok := supportedCrypto(fund.Get("currency").String())
		if !ok {
			continue
		}
		amount, err := decimal.NewFromString(fund.Get("amount").String())
		if err != nil || amount.Sign() <= 0 {
			continue
		}
		id := "pay:" + txID
		if len(funds) > 1 {
			id = fmt.Sprintf("pay:%s:%d", txID, i)
		}
		out = append(out, Receipt{ID: id, Kind: KindPay, Crypto: crypto, Amount: amount, From: from, At: at})
	}

	return out
}

func (b *binance) signedGet(ctx context.Context, cred Credential, path string, query url.Values) ([]byte, error) {
	if cred.ApiKey == "" || cred.ApiSecret == "" {
		return nil, &APIError{Message: "缺少 API Key / Secret", Credential: true}
	}

	status, body, err := doWithFailover(ctx, b.client, b.endpoints, func(base string) (*http.Request, error) {
		q := url.Values{}
		for k, v := range query {
			q[k] = v
		}
		q.Set("recvWindow", binanceRecvWindow)
		q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
		encoded := q.Encode()
		mac := hmac.New(sha256.New, []byte(cred.ApiSecret))
		mac.Write([]byte(encoded))
		encoded += "&signature=" + hex.EncodeToString(mac.Sum(nil))

		req, err := http.NewRequest(http.MethodGet, base+path+"?"+encoded, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-MBX-APIKEY", cred.ApiKey)

		return req, nil
	})
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		res := gjson.ParseBytes(body)
		apiErr := &APIError{Status: status, Code: res.Get("code").String(), Message: res.Get("msg").String()}
		if apiErr.Message == "" {
			apiErr.Message = snippet(body)
		}
		// 401/403 与 -2014/-2015（API-key 格式 / 权限 / IP 白名单）属于凭证问题
		apiErr.Credential = status == http.StatusUnauthorized || status == http.StatusForbidden || apiErr.Code == "-2014" || apiErr.Code == "-2015"
		apiErr.Transient = !apiErr.Credential && status >= 500

		return nil, apiErr
	}

	return body, nil
}
