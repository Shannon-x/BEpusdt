package task

import (
	"context"
	"errors"
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
	Network       string          `json:"network"`
	TxHash        string          `json:"tx_hash"`
	Amount        decimal.Decimal `json:"amount"`
	FromAddress   string          `json:"from_address"`
	RecvAddress   string          `json:"recv_address"`
	Timestamp     time.Time       `json:"timestamp"`
	TradeType     model.TradeType `json:"trade_type"`
	BlockNum      int             `json:"block_num"`
	Index         int             `json:"index"` // 交易内事件序号，与 network+tx_hash 构成流水唯一键
	IndexAssigned bool            `json:"-"`     // Tron/Aptos 旧编码仅在首次持久化时生成，消费批次合并后不得重新编号
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

			// 先把打到本系统钱包的入账落库，再匹配订单：匹配失败也不会丢，对账任务会重新认单
			other, err := processTransferBatch(batch)
			if err != nil {
				log.Task.Warn("process transfers failed, retaining batch for retry:", err)
				continue
			}
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

func processTransferBatch(batch []transfer) ([]transfer, error) {
	if err := persistTransfers(batch); err != nil {
		return nil, err
	}
	orders, err := receivableOrdersForTransfers(batch)
	if err != nil {
		return nil, err
	}
	for _, t := range batch {
		mqttPublish(t)
	}
	other, _, err := matchTransfersWithCount(batch, orders)
	return other, err
}

// matchTransfers 把一批转账与可收款订单匹配，匹配成功的订单进入确认流程并更新流水状态；返回未匹配的转账
func matchTransfers(batch []transfer, orders map[string][]model.Order) []transfer {
	other, _, err := matchTransfersWithCount(batch, orders)
	if err != nil {
		log.Task.Warn("match transfers failed:", err)
		return batch
	}
	return other
}

func matchTransfersWithCount(batch []transfer, orders map[string][]model.Order) ([]transfer, int, error) {
	var other = make([]transfer, 0)
	seen := make(map[string]struct{}, len(batch))
	addresses, err := walletAddressSet()
	if err != nil {
		return nil, 0, err
	}
	events := make([]model.ChainTransfer, 0, len(batch))
	for _, t := range batch {
		if !isWalletAddress(addresses, t.RecvAddress) {
			continue
		}
		events = append(events, model.ChainTransfer{Network: t.Network, TxHash: t.TxHash, EventIndex: t.Index})
	}
	consumed, err := model.MatchedChainTransferKeys(events)
	if err != nil {
		return nil, 0, err
	}
	matchedCount := 0

	for _, t := range batch {
		identity := model.ChainTransferKey(t.Network, t.TxHash, t.Index)
		if _, exists := consumed[identity]; exists {
			continue
		}
		if _, exists := seen[identity]; exists {
			continue
		}
		seen[identity] = struct{}{}
		// 判断数额是否在允许范围内
		if !model.IsAmountValid(t.TradeType, t.Amount) {
			other = append(other, t)
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
			claimed, err := model.MatchChainTransfer(&o, t.Network, t.TxHash, t.Index, t.BlockNum, t.FromAddress, t.RecvAddress, t.Timestamp, t.Amount)
			if errors.Is(err, model.ErrChainTransferAlreadyMatched) {
				matched = true // 并发对账已领取，不是非订单入账
				break
			}
			if err != nil {
				return nil, matchedCount, err
			}
			if !claimed {
				continue
			}

			// 从内存 map 中移除已匹配订单，防止同批次其他 transfer 重复匹配
			orders[key] = append(orderList[:i], orderList[i+1:]...)
			matched = true
			matchedCount++
			break
		}

		if !matched {
			other = append(other, t)
		}
	}

	return other, matchedCount, nil
}

// persistTransfers 把收款地址属于本系统钱包的转账写入链上流水表（幂等）
func persistTransfers(batch []transfer) error {
	if len(batch) == 0 {
		return nil
	}
	addrs, err := walletAddressSet()
	if err != nil {
		return fmt.Errorf("read wallet addresses: %w", err)
	}
	if len(addrs) == 0 {
		return nil
	}

	rows := make([]model.ChainTransfer, 0)
	legacyIndices := make(map[string]int)
	for i, t := range batch {
		if !isWalletAddress(addrs, t.RecvAddress) {
			continue
		}

		// 这两条链的旧版索引是在钱包过滤之后按同交易出现顺序生成。
		// 保持旧编码，且把生成结果同步给消费者，重复落库/合并批次不会制造新事件。
		if t.Network == conf.Tron || t.Network == conf.Aptos {
			key := t.Network + ":" + t.TxHash
			if !t.IndexAssigned {
				t.Index = legacyIndices[key]
				t.IndexAssigned = true
				batch[i] = t
			}
			if t.Index >= legacyIndices[key] {
				legacyIndices[key] = t.Index + 1
			}
		}

		rows = append(rows, model.ChainTransfer{
			Network:     t.Network,
			TxHash:      t.TxHash,
			EventIndex:  t.Index,
			BlockNum:    int64(t.BlockNum),
			FromAddress: t.FromAddress,
			ToAddress:   t.RecvAddress,
			TradeType:   t.TradeType,
			Amount:      t.Amount.String(),
			BlockTime:   t.Timestamp,
			MatchStatus: model.ChainTransferUnmatched,
		})
	}

	return model.SaveChainTransfers(rows)
}

func isWalletAddress(addresses map[string]struct{}, address string) bool {
	_, exact := addresses[address]
	_, lower := addresses[strings.ToLower(address)]
	return exact || lower
}

// walletAddressSet 全部钱包的地址与匹配地址（含小写形式），缓存 10 秒
func walletAddressSet() (map[string]struct{}, error) {
	const key = "wallet_address_set"
	if v, ok := cache.Get(key); ok {
		return v.(map[string]struct{}), nil
	}

	var wallets []model.Wallet
	if err := model.Db.Select("address", "match_addr").Find(&wallets).Error; err != nil {
		return nil, err
	}

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

	return set, nil
}

// reconcileTransfers 对账：回查窗口内付款或补录的未匹配入账，订单按每笔付款当时的有效期选择。
// 覆盖两类情况：匹配时数据库暂时失败；订单先过期、随后补扫到过期前的付款（迟到支付恢复）。
func reconcileTransfers(ctx context.Context) {
	since := time.Now().Add(-reconcileWindow)
	var afterID int64
	var matched int
	for ctx.Err() == nil {
		rows, err := model.UnmatchedChainTransfersPage(since, afterID, 500)
		if err != nil {
			log.Task.Warn("read unmatched chain transfers failed:", err)
			break
		}
		if len(rows) == 0 {
			break
		}
		afterID = rows[len(rows)-1].ID
		batch := make([]transfer, 0, len(rows))
		for _, r := range rows {
			amount, err := decimal.NewFromString(r.Amount)
			if err != nil {
				continue
			}
			batch = append(batch, transfer{
				Network: r.Network, TxHash: r.TxHash, Amount: amount,
				FromAddress: r.FromAddress, RecvAddress: r.ToAddress, Timestamp: r.BlockTime,
				TradeType: r.TradeType, BlockNum: int(r.BlockNum), Index: r.EventIndex,
			})
		}
		orders, err := receivableOrdersForTransfers(batch)
		if err != nil {
			log.Task.Warn("read receivable orders failed:", err)
			break
		}
		_, n, err := matchTransfersWithCount(batch, orders)
		matched += n
		if err != nil {
			log.Task.Warn("reconcile transfer matching failed:", err)
			break
		}
	}

	if matched > 0 {
		log.Task.Warn(fmt.Sprintf("对账补认单 %d 笔（此前匹配失败或迟到支付）", matched))
		scanNotice("reconcile_matched", 10*time.Minute, "对账补认单",
			fmt.Sprintf("对账任务为 %d 笔此前未匹配的入账补认了订单，订单已进入确认流程。\n如果经常出现，说明实时匹配阶段存在问题，请查看 task.log。", matched))
	}
}

func orderTransferMatch(o model.Order, t transfer) bool {
	if o.Status != model.OrderStatusWaiting && o.Status != model.OrderStatusExpired {
		return false
	}
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

					notifier.NonOrderTransfer(model.TronTransfer{
						Network: t.Network, TxHash: t.TxHash, Amount: t.Amount,
						FromAddress: t.FromAddress, RecvAddress: t.RecvAddress,
						Timestamp: t.Timestamp, TradeType: t.TradeType,
						BlockNum: t.BlockNum, Index: t.Index,
					}, wa)
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

// receivableOrdersSince 过期时间晚于指定时刻的可收款订单（待支付 / 已过期），按 匹配地址+交易类型 分组。
func receivableOrdersSince(expiredAfter time.Time) map[string][]model.Order {
	var orders []model.Order
	db := model.Db.Where("status in (?)", []int{model.OrderStatusWaiting, model.OrderStatusExpired}).
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

// receivableOrdersForTransfers 按链上付款时间选单，历史补扫也能找到付款当时尚未过期的旧订单。
// 查询只是缩小范围；每笔仍严格校验地址、币种、金额及创建/过期时间，并由事务保证唯一认领。
func receivableOrdersForTransfers(batch []transfer) (map[string][]model.Order, error) {
	data := make(map[string][]model.Order)
	if len(batch) == 0 {
		return data, nil
	}
	wallets, err := walletAddressSet()
	if err != nil {
		return nil, err
	}
	var minTime, maxTime time.Time
	types := make(map[model.TradeType]struct{})
	addresses := make(map[string]struct{})
	for _, t := range batch {
		if !isWalletAddress(wallets, t.RecvAddress) {
			continue
		}
		if minTime.IsZero() || t.Timestamp.Before(minTime) {
			minTime = t.Timestamp
		}
		if t.Timestamp.After(maxTime) {
			maxTime = t.Timestamp
		}
		types[t.TradeType] = struct{}{}
		addresses[t.RecvAddress] = struct{}{}
	}
	if len(addresses) == 0 {
		return data, nil
	}
	tradeTypes := make([]model.TradeType, 0, len(types))
	for tradeType := range types {
		tradeTypes = append(tradeTypes, tradeType)
	}
	recvAddresses := make([]string, 0, len(addresses))
	for address := range addresses {
		recvAddresses = append(recvAddresses, address)
	}
	var orders []model.Order
	err = model.Db.Where("status in (?) and (ref_hash = '' or ref_hash = trade_id)", []int{model.OrderStatusWaiting, model.OrderStatusExpired}).
		Where("created_at < ? and expired_at > ?", maxTime, minTime).
		Where("trade_type in (?) and (match_address in (?) or (match_address = '' and address in (?)))", tradeTypes, recvAddresses, recvAddresses).
		Order("created_at asc, id asc").Find(&orders).Error
	if err != nil {
		return nil, err
	}
	for _, o := range orders {
		key := orderMatchAddress(o) + string(o.TradeType)
		data[key] = append(data[key], o)
	}
	return data, nil
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

	// 终点：各订单有效期与当前时间交集的最大终点；创建时间顺序不代表过期时间顺序。
	now := time.Now().Unix()
	endAt = startAt
	for _, o := range pending {
		until := min(o.ExpiredAt.Unix(), now)
		if until > endAt {
			endAt = until
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

		if t.SetExpired() {
			notify.Bepusdt(t)
		}
	}
}

const confirmingCacheKey = "confirming_trade_types"

// confirmingTradeTypes 当前存在确认中订单的交易类型集合，缓存 2 秒；十几条链各自每 5 秒轮询时共用一次查询
func confirmingTradeTypes() map[model.TradeType]bool {
	if v, ok := cache.Get(confirmingCacheKey); ok {
		return v.(map[model.TradeType]bool)
	}

	var types []model.TradeType
	model.Db.Model(&model.Order{}).Where("status = ?", model.OrderStatusConfirming).Distinct("trade_type").Pluck("trade_type", &types)
	set := make(map[model.TradeType]bool, len(types))
	for _, t := range types {
		set[t] = true
	}
	cache.Set(confirmingCacheKey, set, 2*time.Second)

	return set
}

func getConfirmingOrders(tradeType []model.TradeType) []model.Order {
	var orders = make([]model.Order, 0)
	var data = make([]model.Order, 0)

	if len(tradeType) > 0 { // 没有该链的确认中订单，直接返回，不查询
		set := confirmingTradeTypes()
		hit := false
		for _, t := range tradeType {
			if set[t] {
				hit = true

				break
			}
		}
		if !hit {
			return data
		}
	}

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
		t, err := decimal.NewFromString(target)
		return err == nil && amount.Equal(t)
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
