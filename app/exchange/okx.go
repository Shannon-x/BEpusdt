package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
)

// OKX：
//   - GET /api/v5/account/config     校验凭证并取得 uid
//   - GET /api/v5/asset/bills        资金账户流水；type 1 = 充值（含内部转账入账），72 = 收到转账；balChg > 0 为入账
//
// 签名：OK-ACCESS-SIGN = base64(HMAC-SHA256(secret, timestamp + "GET" + requestPath))，timestamp 为 ISO8601 毫秒。

var okxCurrencies = []string{"USDT", "USDC"}

type okx struct {
	endpoints []string
	client    *http.Client
}

func (o *okx) Name() string { return "okx" }

func (o *okx) VerifyUID(ctx context.Context, cred Credential) (string, error) {
	data, err := o.signedGet(ctx, cred, "/api/v5/account/config", nil)
	if err != nil {
		return "", err
	}

	uid := data.Get("0.uid").String()
	if uid == "" {
		return "", &APIError{Message: "账户接口未返回 uid", Credential: true}
	}

	return uid, nil
}

func (o *okx) Receipts(ctx context.Context, cred Credential, _ string, since, until time.Time) ([]Receipt, error) {
	receipts := make([]Receipt, 0)
	for _, ccy := range okxCurrencies {
		data, err := o.signedGet(ctx, cred, "/api/v5/asset/bills", url.Values{
			"ccy":   {ccy},
			"limit": {"100"},
			"begin": {strconv.FormatInt(since.UnixMilli(), 10)},
			"end":   {strconv.FormatInt(until.UnixMilli(), 10)},
		})
		if err != nil {
			return nil, err
		}

		for _, row := range data.Array() {
			billType := row.Get("type").String()
			if billType != "1" && billType != "72" {
				continue
			}
			amount, err := decimal.NewFromString(row.Get("balChg").String())
			if err != nil || amount.Sign() <= 0 {
				continue
			}
			crypto, ok := supportedCrypto(row.Get("ccy").String())
			if !ok {
				continue
			}

			receipts = append(receipts, Receipt{
				ID:     "bill:" + row.Get("billId").String(),
				Kind:   KindBill,
				Crypto: crypto,
				Amount: amount,
				At:     unixTime(row.Get("ts").Int()),
			})
		}
	}

	return receipts, nil
}

func (o *okx) signedGet(ctx context.Context, cred Credential, path string, query url.Values) (gjson.Result, error) {
	if cred.ApiKey == "" || cred.ApiSecret == "" || cred.Passphrase == "" {
		return gjson.Result{}, &APIError{Message: "缺少 API Key / Secret / Passphrase", Credential: true}
	}

	requestPath := path
	if len(query) > 0 {
		requestPath += "?" + query.Encode()
	}

	status, body, err := doWithFailover(ctx, o.client, o.endpoints, func(base string) (*http.Request, error) {
		ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		mac := hmac.New(sha256.New, []byte(cred.ApiSecret))
		mac.Write([]byte(ts + http.MethodGet + requestPath))

		req, err := http.NewRequest(http.MethodGet, base+requestPath, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("OK-ACCESS-KEY", cred.ApiKey)
		req.Header.Set("OK-ACCESS-PASSPHRASE", cred.Passphrase)
		req.Header.Set("OK-ACCESS-SIGN", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
		req.Header.Set("OK-ACCESS-TIMESTAMP", ts)

		return req, nil
	})
	if err != nil {
		return gjson.Result{}, err
	}

	res := gjson.ParseBytes(body)
	code := res.Get("code").String()
	if status != http.StatusOK || (code != "" && code != "0") {
		apiErr := &APIError{Status: status, Code: code, Message: res.Get("msg").String()}
		if apiErr.Message == "" {
			apiErr.Message = snippet(body)
		}
		// 401 及 50xxx 系列鉴权错误（50111 无效 Key、50113 签名错误、50114 时间戳、50105 Passphrase 错误、50110 IP 白名单）
		apiErr.Credential = status == http.StatusUnauthorized || status == http.StatusForbidden ||
			code == "50111" || code == "50113" || code == "50105" || code == "50110" || code == "50114" || code == "50100"
		apiErr.Transient = !apiErr.Credential && (status >= 500 || code == "50011") // 50011 限流

		return gjson.Result{}, apiErr
	}

	return res.Get("data"), nil
}
