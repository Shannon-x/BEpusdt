package task

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/smallnest/chanx"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/notifier"
	"github.com/v03413/bepusdt/app/task/notify"
	"github.com/v03413/go-cache"
	"github.com/v03413/tronprotocol/core"
)

type transfer struct {
	Network     string          `json:"network"`
	TxHash      string          `json:"tx_hash"`
	Amount      decimal.Decimal `json:"amount"`
	FromAddress string          `json:"from_address"`
	RecvAddress string          `json:"recv_address"`
	Timestamp   time.Time       `json:"timestamp"`
	TradeType   model.TradeType `json:"trade_type"`
	BlockNum    int             `json:"block_num"`
	Index       int             `json:"index"` // 交易内事件序号，与 network+tx_hash 构成流水唯一键
}

type resource struct {
	ID           string
	Type         core.Transaction_Contract_ContractType
	Balance      int64
	FromAddress  string
	RecvAddress  string
	Timestamp    time.Time
	ResourceCode core.ResourceCode
}

var resourceQueue = chanx.NewUnboundedChan[[]resource](context.Background(), 30) // 资源队列
var notOrderQueue = chanx.NewUnboundedChan[[]transfer](context.Background(), 30) // 非订单队列
var transferQueue = chanx.NewUnboundedChan[[]transfer](context.Background(), 30) // 交易转账队列

const batchInterval = time.Second * 1       // 批处理缓解数据库读取压力
const orderCheckInterval = time.Second * 10 // 订单过期检查间隔
const reconcileInterval = time.Minute * 5   // 对账间隔
const reconcileWindow = time.Hour * 48      // 对账回看窗口：覆盖商户允许重新打开订单的时长并留余量

func init() {
	Register(Task{Callback: orderTransferHandle})
	Register(Task{Callback: notOrderTransferHandle})
	Register(Task{Callback: tronResourceHandle})
	Register(Task{Duration: reconcileInterval, Callback: reconcileTransfers})
}

func orderTransferHandle(ctx context.Context) {
	var batch = make([]transfer, 0, 1000)
	var lastCheckTime = time.Now()
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case transfers, ok := <-transferQueue.Out:
			if !ok {
				return
			}
			batch = append(batch, transfers...)
		case <-ticker.C:
			// 每10秒强制检查一次过期订单，即使没有交易，防止无交易时订单不过期
			var shouldCheck = time.Since(lastCheckTime) >= orderCheckInterval
			if shouldCheck {
				lastCheckTime = time.Now()
			}

			if len(batch) == 0 {
				if shouldCheck {
					expireWaitingOrders()
				}

				continue
			}

			for _, t := range batch {
				mqttPublish(t)
			}

			// 先把打到本系统钱包的入账落库，再匹配订单：匹配失败也不会丢，对账任务会重新认单
			persistTransfers(batch)

			other := matchTransfers(batch, getReceivableOrders())
			if len(other) > 0 {
				notOrderQueue.In <- other
			}

			batch = batch[:0]

			if shouldCheck {
				expireWaitingOrders()
			}
		}
	}
}

// matchTransfers 把一批转账与可收款订单匹配，匹配成功的订单进入确认流程并更新流水状态；返回未匹配的转账
func matchTransfers(batch []transfer, orders map[string][]model.Order) []transfer {
	var other = make([]transfer, 0)

	for _, t := range batch {
		// 判断数额是否在允许范围内
		if !model.IsAmountValid(t.TradeType, t.Amount) {
			continue
		}

		key := fmt.Sprintf("%s%s", t.RecvAddress, t.TradeType)
		orderList, ok := orders[key]
		if !ok {
			other = append(other, t)
			continue
		}

		var matched bool
		for i, o := range orderList {
			if !orderTransferMatch(o, t) {
				continue
			}

			// 订单匹配 进入确认流程
			if err := o.MarkConfirming(t.BlockNum, t.FromAddress, t.TxHash, t.Timestamp, t.Amount); err != nil {
				log.Task.Warn("mark order confirming failed:", err)
				continue
			}

			if err := model.MarkChainTransferMatched(t.Network, t.TxHash, t.Index, o.ID); err != nil {
				log.Task.Warn("mark chain transfer matched failed:", err)
			}

			// 从内存 map 中移除已匹配订单，防止同批次其他 transfer 重复匹配
			orders[key] = append(orderList[:i], orderList[i+1:]...)
			matched = true
			break
		}

		if !matched {
			other = append(other, t)
		}
	}

	return other
}

// persistTransfers 把收款地址属于本系统钱包的转账写入链上流水表（幂等）
func persistTransfers(batch []transfer) {
	addrs := walletAddressSet()
	if len(addrs) == 0 {
		return
	}

	rows := make([]model.ChainTransfer, 0)
	seen := make(map[string]int) // 解析器未提供事件序号的链，同一交易内按出现顺序编号，保证唯一
	for _, t := range batch {
		if _, ok := addrs[t.RecvAddress]; !ok {
			if _, ok := addrs[strings.ToLower(t.RecvAddress)]; !ok {
				continue
			}
		}

		index := t.Index
		key := t.Network + t.TxHash
		if index == 0 {
			index = seen[key]
		}
		seen[key] = index + 1

		rows = append(rows, model.ChainTransfer{
			Network:     t.Network,
			TxHash:      t.TxHash,
			EventIndex:  index,
			BlockNum:    int64(t.BlockNum),
			FromAddress: t.FromAddress,
			ToAddress:   t.RecvAddress,
			TradeType:   t.TradeType,
			Amount:      t.Amount.String(),
			BlockTime:   t.Timestamp,
			MatchStatus: model.ChainTransferUnmatched,
		})
	}

	if err := model.SaveChainTransfers(rows); err != nil {
		log.Task.Warn("save chain transfers failed:", err)
	}
}

// walletAddressSet 全部钱包的地址与匹配地址（含小写形式），缓存 10 秒
func walletAddressSet() map[string]struct{} {
	const key = "wallet_address_set"
	if v, ok := cache.Get(key); ok {
		return v.(map[string]struct{})
	}

	var wallets []model.Wallet
	model.Db.Select("address", "match_addr").Find(&wallets)

	set := make(map[string]struct{}, len(wallets)*2)
	for _, w := range wallets {
		for _, a := range []string{w.Address, w.MatchAddr} {
			if a == "" {
				continue
			}
			set[a] = struct{}{}
			set[strings.ToLower(a)] = struct{}{}
		}
	}
	cache.Set(key, set, 10*time.Second)

	return set
}

// reconcileTransfers 对账：把窗口内仍未匹配订单的入账重新跑一遍订单匹配（订单选择范围放宽到整个对账窗口）。
// 覆盖两类情况：匹配时数据库暂时失败；订单先过期、随后补扫到过期前的付款（迟到支付恢复）。
func reconcileTransfers(context.Context) {
	rows := model.UnmatchedChainTransfers(time.Now().Add(-reconcileWindow), 500)
	if len(rows) == 0 {
		return
	}

	batch := make([]transfer, 0, len(rows))
	for _, r := range rows {
		amount, err := decimal.NewFromString(r.Amount)
		if err != nil {
			continue
		}
		batch = append(batch, transfer{
			Network:     r.Network,
			TxHash:      r.TxHash,
			Amount:      amount,
			FromAddress: r.FromAddress,
			RecvAddress: r.ToAddress,
			Timestamp:   r.BlockTime,
			TradeType:   r.TradeType,
			BlockNum:    int(r.BlockNum),
			Index:       r.EventIndex,
		})
	}

	other := matchTransfers(batch, receivableOrdersSince(time.Now().Add(-reconcileWindow)))
	if matched := len(batch) - len(other); matched > 0 {
		log.Task.Warn(fmt.Sprintf("对账补认单 %d 笔（此前匹配失败或迟到支付）", matched))
		scanAlert("reconcile_matched", 10*time.Minute, "对账补认单",
			fmt.Sprintf("对账任务为 %d 笔此前未匹配的入账补认了订单，订单已进入确认流程。\n如果经常出现，说明实时匹配阶段存在问题，请查看 task.log。", matched))
	}
}

func orderTransferMatch(o model.Order, t transfer) bool {
	if o.TradeType != t.TradeType || orderMatchAddress(o) != t.RecvAddress {
		return false
	}
	if !o.AddressLocked && !amountMatch(t.Amount, o.Amount, string(o.TradeType)) {
		return false
	}
	if !o.CreatedAt.Before(t.Timestamp) || !o.ExpiredAt.After(t.Timestamp) {
		return false
	}

	return true
}

func orderMatchAddress(o model.Order) string {
	if o.MatchAddress != "" {
		return o.MatchAddress
	}

	return o.Address
}

func notOrderTransferHandle(ctx context.Context) {
	var batch = make([]transfer, 0, 1000)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case transfers, ok := <-notOrderQueue.Out:
			if !ok {
				return
			}
			batch = append(batch, transfers...)
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			var was = make([]model.Wallet, 0)
			model.Db.Where("other_notify = ?", model.WaOtherEnable).Find(&was)
			for _, wa := range was {
				for _, t := range batch {
					if t.RecvAddress != wa.MatchAddr && t.FromAddress != wa.MatchAddr {
						continue
					}

					if !model.IsNeedNotifyByTxid(t.TxHash) {
						continue
					}

					var record = model.NotifyRecord{Txid: t.TxHash}
					model.Db.Create(&record)

					notifier.NonOrderTransfer(model.TronTransfer(t), wa)
				}
			}

			batch = batch[:0]
		}
	}
}

func tronResourceHandle(ctx context.Context) {
	var batch = make([]resource, 0, 1000)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case resources, ok := <-resourceQueue.Out:
			if !ok {
				return
			}
			batch = append(batch, resources...)
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			var was []model.Wallet
			model.Db.Where("status = ? and other_notify = ?", model.WaStatusEnable, model.WaOtherEnable).Find(&was)

			for _, wa := range was {
				if wa.GetNetwork() != conf.Tron {
					// 只有 Tron 网络目前才有资源变更通知
					continue
				}

				for _, r := range batch {
					if r.RecvAddress != wa.Address && r.FromAddress != wa.Address {
						continue
					}
					if r.ResourceCode != core.ResourceCode_ENERGY {
						continue
					}
					if !model.IsNeedNotifyByTxid(r.ID) {
						continue
					}

					var record = model.NotifyRecord{Txid: r.ID}
					model.Db.Create(&record)

					notifier.TronResourceChange(model.TronResource(r))
				}
			}

			batch = batch[:0]
		}
	}
}

// markFinalConfirmed 订单成功：状态与回调事件同一事务落库，随后立即尝试回调；失败由 outbox 重试
func markFinalConfirmed(o model.Order) {
	if err := o.SetSuccessWithNotify(); err != nil {
		log.Task.Error(fmt.Sprintf("订单 %s 标记成功失败：%v", o.TradeId, err))

		return
	}

	notifyOrderSuccess(o)
}

func receivableOrderStatuses() []int {
	return []int{model.OrderStatusWaiting, model.OrderStatusExpired, model.OrderStatusConfirming}
}

func getReceivableOrders() map[string][]model.Order {
	return receivableOrdersSince(time.Now().Add(model.GetLookbackHour()))
}

// receivableOrdersSince 过期时间晚于指定时刻的可收款订单（待支付 / 已过期 / 确认中），按 匹配地址+交易类型 分组
func receivableOrdersSince(expiredAfter time.Time) map[string][]model.Order {
	var orders []model.Order
	db := model.Db.Where("status in (?)", receivableOrderStatuses()).
		Where("expired_at > ?", expiredAfter).
		Order("created_at asc")
	db.Find(&orders)

	data := make(map[string][]model.Order)
	for _, t := range orders {
		key := orderMatchAddress(t) + string(t.TradeType)
		data[key] = append(data[key], t)
	}

	return data
}

func hasLookbackOrders(tradeType []model.TradeType) bool {
	var count int64
	db := model.Db.Model(&model.Order{}).
		Where("status in (?)", receivableOrderStatuses()).
		Where("expired_at > ?", time.Now().Add(model.GetLookbackHour()))
	if len(tradeType) > 0 {
		db = db.Where("trade_type in (?)", tradeType)
	}

	db.Count(&count)

	return count > 0
}

// beginLookback 计算待回溯订单的时间范围并开启回溯任务追踪。
// 调用方需为每个入队区块调用 lookbackTrack.track，全部入队后调用 sealed，中断时调用 abort；
// 区块扫描结束时调用 lookbackTrack.done。全部区块成功后订单才会被标记为已回溯。
func beginLookback(network model.Network) (startAt, endAt int64, ok bool) {
	if lookbackTrack.inflight(string(network)) {
		return 0, 0, false
	}

	startAt, endAt, orderIDs, ok := pendingLookbackUnix(network)
	if !ok {
		return 0, 0, false
	}

	if !lookbackTrack.begin(string(network), orderIDs) {
		return 0, 0, false
	}

	return startAt, endAt, true
}

func pendingLookbackUnix(network model.Network) (startAt, endAt int64, orderIDs []int64, ok bool) {
	trade := model.GetNetworkTrades(network)
	if len(trade) == 0 {
		return
	}

	lookback := time.Now().Add(model.GetLookbackHour())
	var all []model.Order
	model.Db.Model(&model.Order{}).
		Where("status in (?) and trade_type in (?)", receivableOrderStatuses(), trade).
		Where("expired_at > ?", lookback).
		Order("created_at asc").
		Find(&all)

	// 过滤掉已经回溯过或正在回溯的订单
	pending := make([]model.Order, 0, len(all))
	for _, o := range all {
		if !lookbackTrack.skip(o.ID) {
			pending = append(pending, o)
		}
	}
	if len(pending) == 0 {
		return
	}
	orderIDs = make([]int64, 0, len(pending))
	for _, o := range pending {
		orderIDs = append(orderIDs, o.ID)
	}

	// 起点：最早的创建时间（已按 created_at asc 排序）
	startAt = pending[0].CreatedAt.Time().Unix()

	// 终点：最晚的已过期 expired_at；若全部尚未过期则用当前时间
	endAt = time.Now().Unix()
	for _, o := range pending {
		if o.ExpiredAt.Before(time.Now()) && o.ExpiredAt.Unix() > startAt {
			endAt = o.ExpiredAt.Unix()
		}
	}

	ok = true

	return
}

func markLookbackDone(orderIDs []int64) {
	lookbackTrack.markDone(orderIDs)
}

func expireWaitingOrders() {
	for _, t := range model.GetOrderByStatus(model.OrderStatusWaiting) {
		if time.Now().Unix() < t.ExpiredAt.Unix() {
			continue
		}

		t.SetExpired()
		notify.Bepusdt(t)
	}
}

func getConfirmingOrders(tradeType []model.TradeType) []model.Order {
	var orders = make([]model.Order, 0)
	var data = make([]model.Order, 0)
	var db = model.Db.Where("status = ?", model.OrderStatusConfirming)
	if len(tradeType) > 0 {
		db = db.Where("trade_type in (?)", tradeType)
	}

	db.Find(&orders)

	for _, order := range orders {
		if time.Now().Unix() >= order.ExpiredAt.Unix() {
			if order.ConfirmedAt == nil || order.ConfirmedAt.IsZero() || !order.ConfirmedAt.Before(order.ExpiredAt) {
				order.SetFailed()
				notify.Bepusdt(order)

				continue
			}
		}

		data = append(data, order)
	}

	return data
}

func amountMatch(amount decimal.Decimal, target, tradeType string) bool {
	mode := model.GetC(model.PaymentMatchMode)
	switch model.MatchMode(mode) {
	case model.Classic:
		return amount.String() == target
	case model.HasPrefix:
		s := amount.String()
		if !strings.HasPrefix(s, target) {
			return false
		}
		rest := s[len(target):]
		if rest == "" {
			return true
		}

		return strings.Contains(target, ".") || strings.HasPrefix(rest, ".")
	case model.RoundOff:
		t, err := decimal.NewFromString(target)
		if err != nil {
			log.Warn(err.Error())

			return false
		}

		_, precision := model.GetAtomicity(model.TradeType(tradeType)) // 标准精度
		precision2 := abs(t.Exponent())                                // 实际精度
		if precision2 != precision {
			precision = precision2
		}

		a := amount.Round(precision)
		t = t.Round(precision)

		return a.Equal(t)
	}

	return false
}

func abs(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}
