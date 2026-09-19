package task

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/exchange"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

// 交易所内部转账监控（币安 / 欧易）。
//
// 内部转账不经过区块链，只能通过交易所只读 API 读取账户入账：
//   - 与链上扫描共用后续管线：入账 → transferQueue → 流水落库 → 订单匹配 → 确认 → 回调 outbox；
//   - 只在存在待支付/回溯窗口内的交易所订单或开启了钱包监控时才请求 API，空闲不消耗额度；
//   - 每次拉取带重叠窗口，按交易所侧 ID 去重；游标记录最近一次成功轮询时间，重启后从游标附近续拉；
//   - 交易所入账即终态，确认阶段直接标记成功。

const (
	exchangePollInterval  = 10 * time.Second
	exchangeOverlap       = 10 * time.Minute // 每次拉取回看的重叠窗口，容忍交易所侧延迟入账
	exchangeFirstLookback = 2 * time.Hour    // 首次启用时回看的窗口
	exchangeMaxLookback   = 24 * time.Hour   // 长时间未轮询后最多回看
	exchangeSeenTTL       = 48 * time.Hour
	exchangeSeenMax       = 20000
)

type exchangeScanner struct {
	network  string
	client   *http.Client
	seen     *seenSet
	lastPoll atomic.Int64 // 最近一次成功轮询的 unix 秒
}

func init() {
	for _, network := range []string{conf.Binance, conf.Okx} {
		s := newExchangeScanner(network)
		Register(Task{Callback: s.poll, Duration: exchangePollInterval})
		Register(Task{Callback: s.confirm, Duration: time.Second * 5})
		registerScanner(network, s.status, s.replay)
	}
}

func newExchangeScanner(network string) *exchangeScanner {
	return &exchangeScanner{
		network: network,
		client:  utils.NewHttpClient(),
		seen:    newSeenSet(exchangeSeenTTL, exchangeSeenMax),
	}
}

func (s *exchangeScanner) newClient() (exchange.Client, error) {
	return exchange.New(s.network, model.Endpoints(model.Network(s.network)), s.client)
}

// poll 轮询所有已配置凭证的交易所钱包
func (s *exchangeScanner) poll(ctx context.Context) {
	if !scanRequired(s.network) {
		return
	}

	wallets := model.GetExchangeWallets(model.Network(s.network))
	if len(wallets) == 0 {
		return
	}

	cli, err := s.newClient()
	if err != nil {
		log.Task.Warn(fmt.Sprintf("%s 交易所客户端初始化失败：%v", s.network, err))

		return
	}

	now := time.Now()
	since := s.pollSince(now)
	allOK := true
	for _, w := range wallets {
		if !s.pollWallet(ctx, cli, w, since, now, false) {
			allOK = false
		}
	}

	if allOK {
		conf.RecordSuccess(s.network, cast.ToString(now.Unix()))
		cursorOf(s.network).reset(now.Unix())
		s.lastPoll.Store(now.Unix())
	}
}

// pollSince 本次拉取的起点：上次成功轮询时间减去重叠窗口；首次启用只回看 2 小时，长时间停机最多回看 24 小时
func (s *exchangeScanner) pollSince(now time.Time) time.Time {
	last := cursorOf(s.network).load()
	if last <= 0 {
		return now.Add(-exchangeFirstLookback)
	}

	since := time.Unix(last, 0).Add(-exchangeOverlap)
	if floor := now.Add(-exchangeMaxLookback); since.Before(floor) {
		since = floor
	}

	return since
}

// pollWallet 拉取一个钱包的入账并送入转账管线；force 为 true 时忽略去重（回放）
func (s *exchangeScanner) pollWallet(ctx context.Context, cli exchange.Client, w model.Wallet, since, until time.Time, force bool) bool {
	cred, ok := w.GetCredentials()
	if !ok {
		return true
	}

	entry := log.Task.WithFields(logrus.Fields{
		"network": s.network,
		"method":  "receipts",
		"wallet":  w.ID,
		"uid":     w.MatchAddr,
		"since":   since.Format(time.DateTime),
	})

	receipts, err := cli.Receipts(ctx, exchange.Credential{ApiKey: cred.ApiKey, ApiSecret: cred.ApiSecret, Passphrase: cred.Passphrase}, w.MatchAddr, since, until)
	if err != nil {
		conf.RecordFailure(s.network)
		entry.WithField("error", err.Error()).Warn("exchange receipts request failed")
		if exchange.IsCredentialError(err) {
			scanAlert(fmt.Sprintf("exchange_cred_%s_%d", s.network, w.ID), time.Hour, "交易所 API 凭证无效",
				fmt.Sprintf("交易所：%s\n钱包：%s（UID %s）\n错误：%s\n该账户的入账将无法识别，请在后台钱包管理中更新只读 API 凭证并确认 IP 白名单。", s.network, w.Name, w.MatchAddr, err.Error()))
		}

		return false
	}

	batch := make([]transfer, 0, len(receipts))
	for _, r := range receipts {
		if !force && !s.seen.add(s.network+":"+w.MatchAddr+":"+r.ID) {
			continue
		}
		tradeType, ok := model.ExchangeTradeType(model.Network(s.network), model.Crypto(r.Crypto))
		if !ok {
			continue
		}

		batch = append(batch, transfer{
			Network:     s.network,
			TxHash:      s.network + ":" + r.ID,
			Amount:      r.Amount,
			FromAddress: r.From,
			RecvAddress: w.MatchAddr,
			Timestamp:   r.At,
			TradeType:   tradeType,
		})
	}

	if len(batch) > 0 {
		transferQueue.In <- batch
		entry.WithField("count", len(batch)).Info("exchange receipts queued")
	}

	return true
}

// confirm 交易所入账即终态：确认中的订单直接标记成功
func (s *exchangeScanner) confirm(context.Context) {
	for _, o := range getConfirmingOrders(model.GetNetworkTrades(model.Network(s.network))) {
		markFinalConfirmed(o)
	}
}

func (s *exchangeScanner) status() ScanStatus {
	endpoints := model.Endpoints(model.Network(s.network))
	endpoint := ""
	if len(endpoints) > 0 {
		endpoint = endpoints[0]
	}

	return ScanStatus{
		Network:    s.network,
		HeadHeight: s.lastPoll.Load(),
		Endpoint:   endpoint,
		Endpoints:  endpoints,
	}
}

// replay 交易所的"区块"是时间：from / to 为 unix 秒，重新拉取该时间段内所有钱包的入账（忽略去重，流水表与订单匹配天然幂等）
func (s *exchangeScanner) replay(from, to, jobID int64) int {
	go func() {
		cli, err := s.newClient()
		if err != nil {
			if jobID != 0 {
				scanJobPartFailed(jobID, err.Error())
			}

			return
		}

		ok := true
		for _, w := range model.GetExchangeWallets(model.Network(s.network)) {
			if !s.pollWallet(context.Background(), cli, w, time.Unix(from, 0), time.Unix(to, 0), true) {
				ok = false
			}
		}

		if jobID != 0 {
			if ok {
				scanJobPartDone(jobID)
			} else {
				scanJobPartFailed(jobID, "部分钱包拉取失败")
			}
		}
	}()

	return 1
}

// seenSet 带过期时间、容量上限的已处理集合
type seenSet struct {
	mu    sync.Mutex
	ttl   time.Duration
	max   int
	items map[string]time.Time
}

func newSeenSet(ttl time.Duration, max int) *seenSet {
	return &seenSet{ttl: ttl, max: max, items: make(map[string]time.Time)}
}

// add 首次出现返回 true 并记录；已存在返回 false
func (s *seenSet) add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if len(s.items) >= s.max {
		for k, t := range s.items {
			if now.Sub(t) > s.ttl {
				delete(s.items, k)
			}
		}
		if len(s.items) >= s.max { // 仍然过大：整体清空，宁可重复处理（下游幂等）也不无限增长
			s.items = make(map[string]time.Time)
		}
	}

	if t, ok := s.items[key]; ok && now.Sub(t) <= s.ttl {
		return false
	}
	s.items[key] = now

	return true
}
