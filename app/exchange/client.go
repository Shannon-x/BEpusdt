// Package exchange 通过交易所只读 API 读取账户入账，用于识别交易所内部转账（不经过区块链的收款）。
//
// 借鉴 HashPay（Apache-2.0）的做法：Binance 读取 Binance Pay 交易记录并以收款方 Binance ID 过滤，
// 另加内部转账充值记录；OKX 读取资金账户流水（类型 1 充值 / 72 收到转账）；保存凭证时用账户接口校验 UID。
package exchange

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	requestTimeout = 8 * time.Second
	maxBody        = 4 << 20

	KindPay             = "pay"              // Binance Pay 收款（Pay ID / 邮箱 / 手机）
	KindInternalDeposit = "internal_deposit" // 提现到本账户充值地址、由交易所内部划转
	KindBill            = "bill"             // OKX 资金账户流水
)

// Receipt 交易所账户的一笔入账
type Receipt struct {
	ID     string          // 交易所侧唯一标识（含来源前缀），用于去重
	Kind   string          // pay / internal_deposit / bill
	Crypto string          // USDT / USDC
	Amount decimal.Decimal // 入账数额
	From   string          // 付款方标识（UID / 邮箱 / 手机），可能为空
	At     time.Time       // 入账时间
}

// Credential 只读 API 凭证
type Credential struct {
	ApiKey     string
	ApiSecret  string
	Passphrase string // OKX 专用
}

// Client 交易所客户端
type Client interface {
	Name() string
	// VerifyUID 校验凭证并返回其所属账户 UID
	VerifyUID(ctx context.Context, cred Credential) (string, error)
	// Receipts 读取 [since, until] 内属于 uid 账户的入账
	Receipts(ctx context.Context, cred Credential, uid string, since, until time.Time) ([]Receipt, error)
}

// APIError 交易所返回的错误
type APIError struct {
	Status     int    // HTTP 状态码
	Code       string // 交易所错误码
	Message    string
	Credential bool // 凭证 / 权限问题：需要人工处理，不应无限重试
	Transient  bool // 限流 / 5xx / 网络：稍后重试或切换域名
}

func (e *APIError) Error() string {
	parts := make([]string, 0, 3)
	if e.Status != 0 {
		parts = append(parts, fmt.Sprintf("HTTP %d", e.Status))
	}
	if e.Code != "" {
		parts = append(parts, "code="+e.Code)
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	if len(parts) == 0 {
		return "exchange api error"
	}

	return strings.Join(parts, " ")
}

// IsCredentialError 是否凭证类错误
func IsCredentialError(err error) bool {
	var e *APIError

	return errors.As(err, &e) && e.Credential
}

// New 按网络创建客户端；endpoints 为 API 基址列表，首个为主，其余在网络错误 / 限流 / 5xx 时依次尝试
func New(network string, endpoints []string, client *http.Client) (Client, error) {
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s: 未配置 API 地址", network)
	}

	switch network {
	case "binance":
		return &binance{endpoints: endpoints, client: client}, nil
	case "okx":
		return &okx{endpoints: endpoints, client: client}, nil
	}

	return nil, fmt.Errorf("不支持的交易所：%s", network)
}

// doWithFailover 依次在各基址上执行请求：网络错误、429、5xx 换下一个；其它响应直接返回
func doWithFailover(ctx context.Context, client *http.Client, endpoints []string, build func(base string) (*http.Request, error)) (int, []byte, error) {
	var lastErr error
	for _, base := range endpoints {
		base = strings.TrimRight(strings.TrimSpace(base), "/")
		if base == "" {
			continue
		}

		req, err := build(base)
		if err != nil {
			return 0, nil, err
		}

		reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		resp, err := client.Do(req.WithContext(reqCtx))
		if err != nil {
			cancel()
			lastErr = &APIError{Message: err.Error(), Transient: true}

			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
		_ = resp.Body.Close()
		cancel()
		if err != nil {
			lastErr = &APIError{Status: resp.StatusCode, Message: err.Error(), Transient: true}

			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = &APIError{Status: resp.StatusCode, Message: snippet(body), Transient: true}

			continue
		}

		return resp.StatusCode, body, nil
	}

	if lastErr == nil {
		lastErr = &APIError{Message: "没有可用的 API 地址", Transient: true}
	}

	return 0, nil, lastErr
}

func snippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > 200 {
		s = s[:200] + "..."
	}

	return s
}

// unixTime 兼容秒 / 毫秒时间戳
func unixTime(v int64) time.Time {
	if v > 10_000_000_000 {
		return time.UnixMilli(v)
	}

	return time.Unix(v, 0)
}

func supportedCrypto(c string) (string, bool) {
	c = strings.ToUpper(strings.TrimSpace(c))
	switch c {
	case "USDT", "USDC":
		return c, true
	}

	return "", false
}
