package task

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/chanx"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	blockapi "github.com/v03413/bepusdt/app/core"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

const (
	defaultBlockBatchSize = 3  // 每次批量请求的区块数量默认值，可通过 block_batch_size 配置
	maxBlockBatchSize     = 50 // 批量上限，避免误配置导致单次请求过大
	evmTransferEvent      = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

var chainBlockNum sync.Map

type block struct {
	RollDelayOffset int64 // 延迟偏移量，某些RPC节点如果不延迟，会报错 block is out of range，目前发现 https://rpc.xlayer.tech/ 存在此问题
	ConfirmedOffset int   // 确认偏移量，开启交易确认后，区块高度需要减去此值认为交易已确认
}

type evmNative struct {
	Parse     bool
	Decimal   int32
	TradeType model.TradeType
}

type evm struct {
	Network          string
	Block            block
	Native           evmNative
	Client           *http.Client
	blockScanQueue   *chanx.UnboundedChan[evmBlock] // 实时区块，高优先级
	lookbackQueue    *chanx.UnboundedChan[evmBlock] // 订单回溯 / 手动回放，低优先级
	LookbackInterval time.Duration                  // 回溯时每批入队的间隔，控制 RPC 调用速率；默认 300ms
	rpc              endpointPicker
	nativeMu         sync.Mutex
	nativeRotate     int // 原生 RPC 单独轮换，套餐限制不影响正常的 ERC20 节点
}

// evmBlock 待扫描区块区间、已失败次数、来源队列及所属持久化任务
type evmBlock struct {
	From       int64
	To         int64
	Attempt    int
	Lookback   bool
	JobID      int64
	NativeOnly bool // 原生币独立补扫，不重复下发已完成的代币流水
}

func newEvm(network string, b block, native evmNative, lookbackInterval time.Duration) *evm {
	ctx := context.Background()

	return &evm{
		Network:          network,
		Block:            b,
		Native:           native,
		Client:           utils.NewHttpClient(),
		blockScanQueue:   chanx.NewUnboundedChan[evmBlock](ctx, 30),
		lookbackQueue:    chanx.NewUnboundedChan[evmBlock](ctx, 30),
		LookbackInterval: lookbackInterval,
		rpc:              endpointPicker{network: model.Network(network)},
	}
}

// registerEvm 注册一条 EVM 链的全部周期任务及状态/回放入口
func registerEvm(e *evm, syncEvery, lookbackEvery time.Duration) {
	Register(Task{Callback: e.blockDispatch})
	Register(Task{Callback: e.syncBlocksForward, Duration: syncEvery})
	Register(Task{Callback: e.tradeConfirmHandle, Duration: time.Second * 5})
	Register(Task{Callback: e.lookbackBlocks, Duration: lookbackEvery})
	registerScanner(e.Network, e.status, e.replay)
}

func (e *evm) queueFor(job evmBlock) *chanx.UnboundedChan[evmBlock] {
	if job.Lookback {
		return e.lookbackQueue
	}

	return e.blockScanQueue
}

func (e *evm) status() ScanStatus {
	var head int64
	if v, ok := chainBlockNum.Load(e.Network); ok {
		head = v.(int64)
	}

	return ScanStatus{
		Network:       e.Network,
		HeadHeight:    head,
		RealtimeQueue: e.blockScanQueue.Len(),
		LookbackQueue: e.lookbackQueue.Len(),
		Endpoint:      e.rpc.current(),
		Endpoints:     e.rpc.list(),
	}
}

// replay 回放 / 任务重试：按批次进入低优先级队列
func (e *evm) replay(from, to, jobID int64) int {
	n := 0
	nativeOnly := false
	if jobID != 0 {
		if job, ok := model.GetScanJob(jobID); ok {
			nativeOnly = job.Kind == model.ScanJobKindNative
		}
	}
	size := e.batchSize()
	for i := from; i <= to; i += size {
		end := i + size - 1
		if end > to {
			end = to
		}
		scanJobPartQueued(jobID, i)
		e.lookbackQueue.In <- evmBlock{From: i, To: end, Lookback: true, JobID: jobID, NativeOnly: nativeOnly}
		n++
	}

	return n
}

// batchSize 每次批量请求的区块数量，不同免费 RPC 对 batch 的限制不同，故可配置
func (e *evm) batchSize() int64 {
	n := cast.ToInt64(model.GetC(model.BlockBatchSize))
	if n <= 0 {
		return defaultBlockBatchSize
	}
	if n > maxBlockBatchSize {
		return maxBlockBatchSize
	}

	return n
}

func (e *evm) syncBlocksForward(ctx context.Context) {
	if syncBreak(e.Network, e.blockScanQueue.Len()) {

		return
	}

	endpoint := e.rpcEndpoint()
	entry := scanLogger(e.Network, endpoint, "eth_getBlockByNumber(latest)", nil)
	// 用最新区块而不是 eth_blockNumber：顺带拿到时间戳，用于识别落后 / 停止同步的节点
	post := []byte(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}`)
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(post))
	if err != nil {
		entry.WithField("error", err.Error()).Warn("syncBlocksForward create request error")

		return
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := e.Client.Do(req)
	if err != nil {
		entry.WithField("error", err.Error()).Warn("syncBlocksForward request error")
		e.rpc.failed(endpoint)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		entry.WithField("error", err.Error()).Warn("syncBlocksForward read body error")
		e.rpc.failed(endpoint)

		return
	}

	if resp.StatusCode != 200 {
		entry.WithFields(logrus.Fields{"http_status": resp.StatusCode, "body": bodySnippet(body)}).Warn("syncBlocksForward http status error")
		e.rpc.failed(endpoint)

		return
	}

	var res = gjson.ParseBytes(body)
	if !gjson.ValidBytes(body) || !res.IsObject() || res.Get("jsonrpc").String() != "2.0" || res.Get("id").Type != gjson.Number || res.Get("id").Float() != 1 {
		entry.WithField("body", bodySnippet(body)).Warn("syncBlocksForward invalid json response")
		e.rpc.failed(endpoint)

		return
	}

	if rpcErr, ok := parseRpcError(res); ok {
		entry.WithFields(rpcErr.fields()).Warn("syncBlocksForward rpc error")
		e.rpc.failed(endpoint)

		return
	}

	head := res.Get("result")
	headNum, validNum := evmQuantity(head.Get("number").String())
	headTimestamp, validTimestamp := evmQuantity(head.Get("timestamp").String())
	now := headNum - e.Block.RollDelayOffset
	if !head.IsObject() || !validNum || !validTimestamp || now <= 0 || headTimestamp <= 0 {
		entry.WithField("body", bodySnippet(body)).Warn("syncBlocksForward invalid block number")
		e.rpc.failed(endpoint)

		return
	}

	headTime := time.Unix(headTimestamp, 0)
	if headIsStale(headTime) {
		entry.WithFields(logrus.Fields{"head": now, "head_time": headTime.Format(time.DateTime)}).Warn("stale head: node is behind, switching endpoint")
		e.rpc.failed(endpoint)

		return
	}

	var lastBlockNumber int64
	if v, ok := chainBlockNum.Load(e.Network); ok {
		lastBlockNumber = v.(int64)
	}

	// 落后节点报告的链头低于已下发高度：忽略，链头只允许前进
	if lastBlockNumber > 0 && now < lastBlockNumber {
		entry.WithFields(logrus.Fields{"head": now, "issued": lastBlockNumber}).Info("head behind issued height (lagging node), ignored")

		return
	}

	// 启动时从持久化游标续扫；链头跳跃超出容忍度时记录 gap 任务后对齐链头
	lastBlockNumber, err = resumeFrom(e.Network, lastBlockNumber, now, blockHeightTolerance())
	if err != nil {
		entry.WithError(err).Error("persist scan gap failed")
		return
	}

	chainBlockNum.Store(e.Network, now)
	if now <= lastBlockNumber {

		return
	}

	cursorOf(e.Network).issue(lastBlockNumber+1, now)
	size := e.batchSize()
	for from := lastBlockNumber + 1; from <= now; from += size {
		to := from + size - 1
		if to > now {
			to = now
		}

		e.blockScanQueue.In <- evmBlock{From: from, To: to}
	}
}

func (e *evm) lookbackBlocks(ctx context.Context) {
	if e.lookbackQueue.Len() >= blockQueueLimit || !scanRequired(e.Network) {
		return
	}

	startAt, endAt, ok := beginLookback(model.Network(e.Network))
	if !ok {
		return
	}

	interval := e.LookbackInterval
	if interval <= 0 {
		interval = time.Millisecond * 300
	}

	start, end := blockapi.New().GetBoundaryHeights(startAt, endAt, e.Network)
	if start <= 0 || end < start {
		log.Task.Warn(fmt.Sprintf("%s 回溯高度范围无效: start=%d end=%d", e.Network, start, end))
		lookbackTrack.abort(e.Network)

		return
	}

	size := e.batchSize()
	for i := start; i <= end; i += size {
		// 回溯走独立低优先级队列，拥堵时等待腾出空间，不再中断任务
		if !waitQueueRoom(ctx, e.lookbackQueue) {
			lookbackTrack.abort(e.Network)

			return
		}
		to := i + size - 1
		if to > end {
			to = end
		}
		lookbackTrack.track(e.Network, i)
		e.lookbackQueue.In <- evmBlock{From: i, To: to, Lookback: true}
		time.Sleep(interval)
	}

	lookbackTrack.sealed(e.Network)
}

func (e *evm) blockDispatch(ctx context.Context) {
	p, err := ants.NewPoolWithFunc(scanWorkers(3), e.getBlockByNumber)
	if err != nil {
		log.Task.Warn("Error creating pool:", err)

		return
	}

	defer p.Release()

	for {
		job, ok := takeJob(ctx, e.blockScanQueue, e.lookbackQueue)
		if !ok {
			return
		}

		if err := p.Invoke(job); err != nil {
			e.queueFor(job).In <- job

			log.Task.Warn("Evm Block Dispatch Error invoking process block:", err)
		}
	}
}

func (e *evm) getBlockByNumber(a any) {
	b, ok := a.(evmBlock)
	if !ok {
		log.Task.Warn("Evm Block Parse Error: expected evmBlock, got", a)

		return
	}

	e.scanBlocks(b)
}

// scanBlocks 先完成代币流水持久化与下发，再独立扫描原生币；受限的 fullTransactions RPC 不会阻塞 ERC20 认单。
func (e *evm) scanBlocks(job evmBlock) {
	endpoint := e.rpc.current()
	entry := scanLogger(e.Network, endpoint, "eth_getBlockByNumber", logrus.Fields{
		"from": job.From, "to": job.To, "attempt": job.Attempt,
		"lookback": job.Lookback, "native_only": job.NativeOnly,
		"queue_length": e.queueFor(job).Len(),
	})
	fail := func(reason string, err error, nodeFailure bool) {
		conf.RecordFailure(e.Network)
		if nodeFailure && (!errors.Is(err, errEvmBlockUnavailable) || unavailableSwitch(job.Attempt)) {
			e.rpc.failed(endpoint)
		}
		e.retryBlocks(job, reason+": "+err.Error(), entry.WithError(err))
	}

	if !job.NativeOnly {
		ctx, cancel := context.WithTimeout(context.Background(), scanRequestTimeout)
		blocks, _, err := e.fetchBlocks(ctx, endpoint, job, false)
		if err != nil {
			cancel()
			fail("block headers failed", err, true)
			return
		}
		transfers, err := e.parseEventTransfer(ctx, endpoint, job, blocks)
		cancel()
		if err != nil {
			fail("eth_getLogs failed", err, true)
			return
		}
		if err := persistTransfers(transfers); err != nil {
			fail("persist token transfers failed", err, false)
			return
		}
		if len(transfers) > 0 {
			transferQueue.In <- transfers
		}
	}

	if e.nativeRequired(job) {
		nativeEndpoint := e.nativeEndpoint()
		ctx, cancel := context.WithTimeout(context.Background(), scanRequestTimeout)
		_, transfers, nativeErr := e.fetchBlocks(ctx, nativeEndpoint, job, true)
		cancel()
		nodeFailure := nativeErr != nil
		if nativeErr == nil {
			nativeErr = persistTransfers(transfers)
		}
		if nativeErr != nil {
			if nodeFailure {
				e.nativeEndpointFailed(nativeEndpoint)
			}
			if job.NativeOnly {
				fail("native scan failed", nativeErr, false)
				return
			}
			// 原生补扫任务先落库，随后才允许代币主扫描推进游标；重启也不会丢失 ETH。
			_, err := model.CreateScanJob(e.Network, job.From, job.To, model.ScanJobKindNative,
				model.ScanJobStatusPending, nativeErr.Error(), time.Now().Add(model.ScanJobRetryBase))
			if err != nil {
				fail("persist native retry job failed", err, false)
				return
			}
			conf.RecordFailure(e.Network)
			entry.WithError(nativeErr).Warn("native scan deferred to durable retry job")
		} else if len(transfers) > 0 {
			transferQueue.In <- transfers
		}
	}

	conf.RecordSuccess(e.Network, cast.ToString(job.To))
	if !job.NativeOnly {
		lookbackTrack.done(e.Network, job.From, true)
	}
	if !job.Lookback {
		cursorOf(e.Network).complete(job.From, job.To)
	}
	if job.JobID != 0 {
		scanJobPartDone(job.JobID, job.From)
	}
	log.Task.Info(fmt.Sprintf("区块扫描完成(%s): %d → %d 成功率：%s", e.Network, job.From, job.To, conf.GetSuccessRate(e.Network)))
}

// nativeRequired 原生币按支付/监控需求扫描；回放还考虑历史钱包和订单，覆盖已过实时窗口的 ETH。
func (e *evm) nativeRequired(job evmBlock) bool {
	if !e.Native.Parse {
		return false
	}
	if job.NativeOnly || mqttSubscribed(e.Network) {
		return true
	}
	var count int64
	wallets := model.Db.Model(&model.Wallet{}).Where("trade_type = ?", e.Native.TradeType)
	if job.JobID == 0 {
		wallets = wallets.Where("other_notify = ?", model.WaOtherEnable)
	}
	if err := wallets.Limit(1).Count(&count).Error; err != nil || count > 0 {
		return true
	}
	// 等待、确认中及回溯窗口内的过期原生订单都需要保留扫描。
	orders := model.Db.Model(&model.Order{}).Where("trade_type = ?", e.Native.TradeType)
	if job.JobID == 0 {
		orders = orders.Where("status in (?)", receivableOrderStatuses()).
			Where("expired_at > ?", time.Now().Add(model.GetLookbackHour()))
	}
	if err := orders.Limit(1).Count(&count).Error; err != nil {
		return true
	}
	return count > 0
}

func (e *evm) nativeEndpoint() string {
	list := e.rpc.list()
	if len(list) == 0 {
		return ""
	}
	e.nativeMu.Lock()
	defer e.nativeMu.Unlock()
	return list[e.nativeRotate%len(list)]
}

func (e *evm) nativeEndpointFailed(endpoint string) {
	list := e.rpc.list()
	if len(list) < 2 {
		return
	}
	e.nativeMu.Lock()
	defer e.nativeMu.Unlock()
	if list[e.nativeRotate%len(list)] == endpoint {
		e.nativeRotate = (e.nativeRotate + 1) % len(list)
	}
}

var errEvmBlockUnavailable = errors.New("block result is null (node behind)")

// fetchBlocks 严格校验完整批次。代币仅取区块头；原生币另取交易对象，能力限制互不影响。
func (e *evm) fetchBlocks(ctx context.Context, endpoint string, job evmBlock, fullTransactions bool) (map[int64]evmBlockRef, []transfer, error) {
	items := make([]string, 0, job.To-job.From+1)
	for i := job.From; i <= job.To; i++ {
		items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",%t],"id":%d}`, i, fullTransactions, i))
	}
	data, err := e.rpcRequest(ctx, endpoint, []byte("["+strings.Join(items, ",")+"]"))
	if err != nil {
		return nil, nil, err
	}
	byID, err := evmBatchResponses(data, job.From, job.To)
	if err != nil {
		return nil, nil, err
	}
	blocks := make(map[int64]evmBlockRef, len(byID))
	transfers := make([]transfer, 0)
	for i := job.From; i <= job.To; i++ {
		result := byID[i].Get("result")
		if !result.Exists() || result.Type == gjson.Null {
			return nil, nil, fmt.Errorf("block %d: %w", i, errEvmBlockUnavailable)
		}
		if !result.IsObject() {
			return nil, nil, fmt.Errorf("block %d result is not an object", i)
		}
		num, ok := evmQuantity(result.Get("number").String())
		if !ok || num != i {
			return nil, nil, fmt.Errorf("block %d number mismatch or invalid", i)
		}
		ts, ok := evmQuantity(result.Get("timestamp").String())
		if !ok || ts <= 0 {
			return nil, nil, fmt.Errorf("block %d timestamp missing or invalid", i)
		}
		hash := result.Get("hash").String()
		if !evmHexSize(hash, 32) {
			return nil, nil, fmt.Errorf("block %d hash missing or invalid", i)
		}
		blockTime := time.Unix(ts, 0)
		blocks[i] = evmBlockRef{Num: i, Hash: hash, Time: blockTime}
		if fullTransactions {
			transactions := result.Get("transactions")
			if !transactions.IsArray() {
				return nil, nil, fmt.Errorf("block %d transactions missing", i)
			}
			for _, tx := range transactions.Array() {
				if !tx.IsObject() || !strings.HasPrefix(tx.Get("input").String(), "0x") ||
					!evmHexSize(tx.Get("hash").String(), 32) || !evmHexSize(tx.Get("from").String(), 20) {
					return nil, nil, fmt.Errorf("block %d full transactions not returned", i)
				}
				if _, ok := evmBigQuantity(tx.Get("value").String()); !ok {
					return nil, nil, fmt.Errorf("block %d transaction value missing or invalid", i)
				}
				if to := tx.Get("to"); to.Type != gjson.Null && !evmHexSize(to.String(), 20) {
					return nil, nil, fmt.Errorf("block %d transaction recipient invalid", i)
				}
			}
			transfers = append(transfers, e.parseNativeTransfer(transactions.Array(), int(i), blockTime)...)
		}
	}
	return blocks, transfers, nil
}

func (e *evm) rpcRequest(ctx context.Context, endpoint string, post []byte) (gjson.Result, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(post))
	if err != nil {
		return gjson.Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.Client.Do(req)
	if err != nil {
		return gjson.Result{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return gjson.Result{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return gjson.Result{}, fmt.Errorf("RPC http status %d: %s", resp.StatusCode, bodySnippet(body))
	}
	if !gjson.ValidBytes(body) {
		return gjson.Result{}, fmt.Errorf("RPC invalid json: %s", bodySnippet(body))
	}
	data := gjson.ParseBytes(body)
	if rpcErr, ok := parseRpcError(data); ok {
		return gjson.Result{}, fmt.Errorf("RPC error code=%d message=%s", rpcErr.Code, rpcErr.Message)
	}
	return data, nil
}

func evmBatchResponses(data gjson.Result, from, to int64) (map[int64]gjson.Result, error) {
	if !data.IsArray() {
		return nil, errors.New("RPC batch response is not an array")
	}
	byID := make(map[int64]gjson.Result)
	for _, item := range data.Array() {
		id := item.Get("id")
		if !item.IsObject() || item.Get("jsonrpc").String() != "2.0" || id.Type != gjson.Number || id.Int() < from || id.Int() > to || id.Float() != float64(id.Int()) {
			return nil, errors.New("RPC batch response has invalid id")
		}
		if _, exists := byID[id.Int()]; exists {
			return nil, errors.New("RPC batch response has duplicate id")
		}
		if rpcErr, ok := parseRpcError(item); ok {
			return nil, fmt.Errorf("RPC block %d error code=%d message=%s", id.Int(), rpcErr.Code, rpcErr.Message)
		}
		byID[id.Int()] = item
	}
	if int64(len(byID)) != to-from+1 {
		return nil, errors.New("RPC batch response missing block")
	}
	return byID, nil
}

func evmQuantity(s string) (int64, bool) {
	n, ok := evmBigQuantity(s)
	if !ok || !n.IsInt64() {
		return 0, false
	}
	return n.Int64(), true
}

func evmBigQuantity(s string) (*big.Int, bool) {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 {
		return nil, false
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return nil, false
		}
	}
	return new(big.Int).SetString(s[2:], 16)
}

func evmHexSize(s string, size int) bool {
	if len(s) != 2+size*2 || !strings.HasPrefix(s, "0x") {
		return false
	}
	_, err := hex.DecodeString(s[2:])
	return err == nil
}

func evmAddressTopic(s string) bool {
	return evmHexSize(s, 32) && strings.HasPrefix(s, "0x000000000000000000000000")
}

// retryBlocks 退避后重新入队；重试耗尽则放弃并通知回溯追踪
func (e *evm) retryBlocks(job evmBlock, reason string, entry *logrus.Entry) {
	job.Attempt++
	if delay, ok := scanRetryLater(e.queueFor(job), job, job.Attempt); ok {
		entry.WithField("next_retry_in", delay.Round(time.Millisecond).String()).Warn("block scan failed, will retry: " + reason)

		return
	}
	if job.NativeOnly && job.JobID != 0 {
		// 原生子任务不结算同高度的普通订单回溯，也不推进代币连续游标。
		scanJobPartFailed(job.JobID, reason, job.From)
		entry.Error("native scan abandoned after max attempts: " + reason)
		scanAlert("native_abandon_"+e.Network, 5*time.Minute, "原生币扫描重试",
			fmt.Sprintf("网络：%s\n原生币任务：#%d 区块 %d → %d\n原因：%s\n已保留持久化任务继续自动重试，请配置支持完整交易对象的 RPC。", e.Network, job.JobID, job.From, job.To, reason))
		return
	}

	scanAbandon(e.Network, job.From, job.To, job.JobID, reason, entry)
}

func (e *evm) parseNativeTransfer(array []gjson.Result, num int, timestamp time.Time) []transfer {
	nativeTransfers := make([]transfer, 0)
	for _, tx := range array {
		if tx.Get("input").String() != "0x" {
			// 非原生币交易

			continue
		}

		valStr := tx.Get("value").String()
		if valStr == "0x0" || len(valStr) < 3 {
			// 过滤 0 值交易

			continue
		}

		amount, ok := big.NewInt(0).SetString(valStr[2:], 16)
		if !ok || amount.Sign() <= 0 {

			continue
		}

		toAddress := tx.Get("to").String()
		if toAddress == "" { // 合约创建交易 to 为空

			continue
		}

		nativeTransfers = append(nativeTransfers, transfer{
			Network:     e.Network,
			FromAddress: tx.Get("from").String(),
			RecvAddress: toAddress,
			Amount:      decimal.NewFromBigInt(amount, e.Native.Decimal),
			TxHash:      tx.Get("hash").String(),
			BlockNum:    num,
			Timestamp:   timestamp,
			TradeType:   e.Native.TradeType,
			Index:       -1, // 每个交易最多一笔原生转账；与非负 ERC20 logIndex 独立
		})
	}

	return nativeTransfers
}

// evmBlockRef 已校验的区块引用：号、哈希、时间
type evmBlockRef struct {
	Num  int64
	Hash string
	Time time.Time
}

// parseEventTransfer 通过 eth_getLogs 拉取 Transfer 事件。
// 每个区块按 blockHash 单独查询并合并成一个 JSON-RPC 批量请求（HTTP 次数与按区间查询相同）：
//   - 日志一定来自已校验的那个块，不受节点池内高度差影响；节点没有该块会返回错误而不是静默返回空；
//   - 请求按当前网络启用的代币合约过滤，避免下载全链所有代币日志。
func (e *evm) parseEventTransfer(ctx context.Context, endpoint string, b evmBlock, blocks map[int64]evmBlockRef) ([]transfer, error) {
	transfers := make([]transfer, 0)

	contracts := model.GetNetworkContracts(model.Network(e.Network))
	if len(contracts) == 0 { // 该网络没有启用任何代币合约，无需拉取日志

		return transfers, nil
	}

	addresses := make([]string, 0, len(contracts))
	for _, c := range contracts {
		addresses = append(addresses, fmt.Sprintf(`"%s"`, c))
	}

	items := make([]string, 0, b.To-b.From+1)
	for i := b.From; i <= b.To; i++ {
		ref, ok := blocks[i]
		if !ok {
			return transfers, fmt.Errorf("block %d missing from validated batch", i)
		}
		items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"blockHash":"%s","address":[%s],"topics":["%s"]}],"id":%d}`,
			ref.Hash, strings.Join(addresses, ","), evmTransferEvent, i))
	}

	data, err := e.rpcRequest(ctx, endpoint, []byte("["+strings.Join(items, ",")+"]"))
	if err != nil {
		return transfers, fmt.Errorf("eth_getLogs: %w", err)
	}
	byId, err := evmBatchResponses(data, b.From, b.To)
	if err != nil {
		return transfers, fmt.Errorf("eth_getLogs: %w", err)
	}

	for i := b.From; i <= b.To; i++ {
		itm, ok := byId[i]
		if !ok {
			return transfers, fmt.Errorf("eth_getLogs batch response missing block %d", i)
		}
		if rpcErr, ok := parseRpcError(itm); ok {
			return transfers, fmt.Errorf("eth_getLogs block %d rpc error code=%d message=%s", i, rpcErr.Code, rpcErr.Message)
		}
		result := itm.Get("result")
		if !result.IsArray() {
			return transfers, fmt.Errorf("eth_getLogs block %d result is not an array", i)
		}

		ref := blocks[i]
		for _, log := range result.Array() {
			to := strings.ToLower(log.Get("address").String())
			tradeType, ok := model.GetContractTrade(to)
			if !ok || model.TradeNetwork(tradeType) != model.Network(e.Network) {
				continue
			}

			topics := log.Get("topics").Array()
			if len(topics) < 1 || !strings.EqualFold(topics[0].String(), evmTransferEvent) { // transfer event signature

				continue
			}

			if len(topics) != 3 || !evmAddressTopic(topics[1].String()) || !evmAddressTopic(topics[2].String()) || !evmHexSize(log.Get("data").String(), 32) {
				return transfers, fmt.Errorf("eth_getLogs block %d returned malformed Transfer event", i)
			}
			fromTopic, recvTopic := topics[1].String(), topics[2].String()
			dataHex := log.Get("data").String()

			amount, ok := big.NewInt(0).SetString(dataHex[2:], 16)
			if !ok || amount.Sign() <= 0 {

				continue
			}

			if bh := log.Get("blockHash").String(); !strings.EqualFold(bh, ref.Hash) {
				return transfers, fmt.Errorf("eth_getLogs block %d returned log of another block hash %s", i, bh)
			}
			if num, ok := evmQuantity(log.Get("blockNumber").String()); !ok || num != i {
				return transfers, fmt.Errorf("eth_getLogs block %d returned invalid log block number", i)
			}
			index, ok := evmQuantity(log.Get("logIndex").String())
			if !ok || !evmHexSize(log.Get("transactionHash").String(), 32) || log.Get("removed").Bool() {
				return transfers, fmt.Errorf("eth_getLogs block %d returned incomplete or removed log", i)
			}

			transfers = append(transfers, transfer{
				Network:     e.Network,
				FromAddress: fmt.Sprintf("0x%s", fromTopic[26:]),
				RecvAddress: fmt.Sprintf("0x%s", recvTopic[26:]),
				Amount:      decimal.NewFromBigInt(amount, model.GetContractDecimal(to)),
				TxHash:      log.Get("transactionHash").String(),
				BlockNum:    int(ref.Num),
				Timestamp:   ref.Time,
				TradeType:   tradeType,
				Index:       int(index),
			})
		}
	}

	return transfers, nil
}

func (e *evm) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(model.GetNetworkTrades(model.Network(e.Network)))
	var wg sync.WaitGroup

	var handle = func(o model.Order) {
		if model.GetC(model.BlockOffsetConfirm) == "1" {
			last, ok := chainBlockNum.Load(e.Network)
			if !ok {
				return
			}
			if cast.ToInt(last)-o.RefBlockNum < e.Block.ConfirmedOffset {
				return
			}
		}

		endpoint := e.rpcEndpoint()
		confirmed, err := e.confirmReceipt(ctx, endpoint, o)
		if err != nil {
			scanLogger(e.Network, endpoint, "eth_getTransactionReceipt", nil).WithError(err).Warn("transaction confirmation failed")
			e.rpc.failed(endpoint)
			return
		}
		if confirmed {
			markFinalConfirmed(o)
		}
	}

	for _, order := range orders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle(order)
		}()
	}

	wg.Wait()
}

// confirmReceipt 只确认同一笔、同一区块的成功交易，代币订单还必须在 receipt 中有对应合约/地址/金额的 Transfer。
func (e *evm) confirmReceipt(ctx context.Context, endpoint string, o model.Order) (bool, error) {
	if !evmHexSize(o.RefHash, 32) || o.RefBlockNum <= 0 || model.TradeNetwork(o.TradeType) != model.Network(e.Network) {
		return false, errors.New("order transaction reference or network is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, scanRequestTimeout)
	defer cancel()
	post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":1}`, o.RefHash))
	data, err := e.rpcRequest(ctx, endpoint, post)
	if err != nil {
		return false, err
	}
	if !data.IsObject() || data.Get("jsonrpc").String() != "2.0" || data.Get("id").Type != gjson.Number || data.Get("id").Float() != 1 {
		return false, errors.New("receipt response is not a matching JSON-RPC object")
	}
	receipt := data.Get("result")
	if !receipt.IsObject() {
		return false, errors.New("transaction receipt missing or invalid")
	}
	if !evmHexSize(receipt.Get("transactionHash").String(), 32) || !strings.EqualFold(receipt.Get("transactionHash").String(), o.RefHash) {
		return false, errors.New("receipt transaction hash mismatch")
	}
	blockNum, ok := evmQuantity(receipt.Get("blockNumber").String())
	if !ok || blockNum != int64(o.RefBlockNum) || !evmHexSize(receipt.Get("blockHash").String(), 32) {
		return false, errors.New("receipt block reference mismatch or invalid")
	}
	status, ok := evmQuantity(receipt.Get("status").String())
	if !ok || status > 1 {
		return false, errors.New("receipt status missing or invalid")
	}
	if status == 0 {
		return false, nil
	}
	if o.TradeType == e.Native.TradeType {
		if !strings.EqualFold(receipt.Get("to").String(), orderMatchAddress(o)) || !strings.EqualFold(receipt.Get("from").String(), o.FromAddress) {
			return false, errors.New("native receipt recipient or sender mismatch")
		}
		return true, nil
	}
	logs := receipt.Get("logs")
	if !logs.IsArray() {
		return false, errors.New("receipt logs missing or invalid")
	}
	for _, event := range logs.Array() {
		contract := strings.ToLower(event.Get("address").String())
		tradeType, ok := model.GetContractTrade(contract)
		if !ok || tradeType != o.TradeType || model.TradeNetwork(tradeType) != model.Network(e.Network) {
			continue
		}
		topics := event.Get("topics").Array()
		if len(topics) != 3 || !strings.EqualFold(topics[0].String(), evmTransferEvent) || !evmAddressTopic(topics[1].String()) || !evmAddressTopic(topics[2].String()) {
			continue
		}
		if !strings.EqualFold("0x"+topics[2].String()[26:], orderMatchAddress(o)) || !strings.EqualFold("0x"+topics[1].String()[26:], o.FromAddress) {
			continue
		}
		if !evmHexSize(event.Get("data").String(), 32) || event.Get("removed").Bool() ||
			!strings.EqualFold(event.Get("transactionHash").String(), o.RefHash) ||
			!strings.EqualFold(event.Get("blockHash").String(), receipt.Get("blockHash").String()) {
			continue
		}
		eventBlock, ok := evmQuantity(event.Get("blockNumber").String())
		if !ok || eventBlock != blockNum {
			continue
		}
		if _, ok := evmQuantity(event.Get("logIndex").String()); !ok {
			continue
		}
		amount, ok := new(big.Int).SetString(event.Get("data").String()[2:], 16)
		if !ok || amount.Sign() <= 0 || !amountMatch(decimal.NewFromBigInt(amount, model.GetContractDecimal(contract)), o.Amount, string(o.TradeType)) {
			continue
		}
		return true, nil
	}
	return false, errors.New("successful receipt is missing the order's Transfer event")
}

func (e *evm) rpcEndpoint() string {
	return e.rpc.current()
}
