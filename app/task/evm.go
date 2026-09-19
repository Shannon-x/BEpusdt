package task

import (
	"bytes"
	"context"
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
}

// evmBlock 待扫描区块区间、已失败次数、来源队列及所属持久化任务
type evmBlock struct {
	From     int64
	To       int64
	Attempt  int
	Lookback bool
	JobID    int64
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
	size := e.batchSize()
	for i := from; i <= to; i += size {
		end := i + size - 1
		if end > to {
			end = to
		}
		e.lookbackQueue.In <- evmBlock{From: i, To: end, Lookback: true, JobID: jobID}
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
	if !res.IsObject() {
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
	var now = utils.HexStr2Int(head.Get("number").String()).Int64() - e.Block.RollDelayOffset
	if now <= 0 {
		entry.WithField("body", bodySnippet(body)).Warn("syncBlocksForward invalid block number")
		e.rpc.failed(endpoint)

		return
	}

	headTime := time.Unix(utils.HexStr2Int(head.Get("timestamp").String()).Int64(), 0)
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
	lastBlockNumber = resumeFrom(e.Network, lastBlockNumber, now, blockHeightTolerance())

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

// scanBlocks 批量拉取并解析一段区块。批量响应必须逐项校验：数组、每个 id 有对应结果、无 error、区块号一致、时间戳存在，
// 随后的 eth_getLogs 也成功，才计为成功；任何一项失败都进入退避重试，不推进成功计数。
func (e *evm) scanBlocks(job evmBlock) {
	endpoint := e.rpc.current()
	entry := scanLogger(e.Network, endpoint, "eth_getBlockByNumber", logrus.Fields{
		"from":         job.From,
		"to":           job.To,
		"attempt":      job.Attempt,
		"lookback":     job.Lookback,
		"queue_length": e.queueFor(job).Len(),
	})

	// fail 记失败并安排重试；switchEndpoint 为 true 表示失败源于节点本身，需要切换备用节点
	fail := func(reason string, fields logrus.Fields, switchEndpoint bool) {
		conf.RecordFailure(e.Network)
		if switchEndpoint {
			e.rpc.failed(endpoint)
		}
		e.retryBlocks(job, reason, entry.WithFields(fields))
	}

	// unavailable 区块尚未可用（节点落后）：前两次在原节点等待，持续不可用才切换；首次等待不计入失败率
	unavailable := func(reason string, fields logrus.Fields) {
		if job.Attempt > 0 {
			conf.RecordFailure(e.Network)
		}
		if unavailableSwitch(job.Attempt) {
			e.rpc.failed(endpoint)
		}
		e.retryBlocks(job, reason, entry.WithFields(fields))
	}

	items := make([]string, 0, job.To-job.From+1)
	for i := job.From; i <= job.To; i++ {
		items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",%t],"id":%d}`, i, e.Native.Parse, i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), scanRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer([]byte(fmt.Sprintf(`[%s]`, strings.Join(items, ",")))))
	if err != nil {
		fail("create request error", logrus.Fields{"error": err.Error()}, false)

		return
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := e.Client.Do(req)
	if err != nil {
		fail("http request error", logrus.Fields{"error": err.Error()}, true)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fail("read response body error", logrus.Fields{"http_status": resp.StatusCode, "error": err.Error()}, true)

		return
	}

	if resp.StatusCode != 200 { // 429 / 5xx 等，退避后重试
		fail("http status error", logrus.Fields{"http_status": resp.StatusCode, "body": bodySnippet(body)}, true)

		return
	}

	res := gjson.ParseBytes(body)
	if !res.IsArray() {
		// 部分 RPC 对批量请求整体报错时返回单个对象
		if rpcErr, ok := parseRpcError(res); ok {
			fail("batch rpc error", rpcErr.fields(), true)

			return
		}

		fail("batch response is not an array", logrus.Fields{"body": bodySnippet(body)}, true)

		return
	}

	byId := make(map[int64]gjson.Result)
	for _, itm := range res.Array() {
		byId[itm.Get("id").Int()] = itm
	}

	nativeTransfers := make([]transfer, 0)
	blocks := make(map[int64]evmBlockRef)
	for i := job.From; i <= job.To; i++ {
		itm, ok := byId[i]
		if !ok {
			fail("batch response missing block", logrus.Fields{"block": i, "received": len(byId)}, true)

			return
		}

		if rpcErr, ok := parseRpcError(itm); ok {
			fail("rpc error", logrus.Fields{"block": i, "rpc_error_code": rpcErr.Code, "rpc_error_message": rpcErr.Message}, true)

			return
		}

		result := itm.Get("result")
		if !result.Exists() || result.Type == gjson.Null {
			// 节点尚未同步到该高度：时序问题，原节点等待，持续不可用才切换
			unavailable("block result is null (node behind)", logrus.Fields{"block": i})

			return
		}

		num := utils.HexStr2Int(result.Get("number").String()).Int64()
		if num != i {
			fail("block number mismatch", logrus.Fields{"block": i, "got": num}, true)

			return
		}

		ts := result.Get("timestamp")
		if !ts.Exists() || ts.String() == "" {
			fail("block timestamp missing", logrus.Fields{"block": i}, true)

			return
		}

		hash := result.Get("hash").String()
		if hash == "" {
			fail("block hash missing", logrus.Fields{"block": i}, true)

			return
		}

		blockTime := time.Unix(utils.HexStr2Int(ts.String()).Int64(), 0)
		blocks[i] = evmBlockRef{Num: i, Hash: hash, Time: blockTime}

		var array = result.Get("transactions").Array()
		if e.Native.Parse && len(array) != 0 {

			nativeTransfers = append(nativeTransfers, e.parseNativeTransfer(array, int(i), blockTime)...)
		}
	}

	transfers, err := e.parseEventTransfer(ctx, endpoint, job, blocks)
	if err != nil {
		fail("eth_getLogs failed", logrus.Fields{"error": err.Error()}, true)

		return
	}

	if len(nativeTransfers) > 0 {
		transferQueue.In <- nativeTransfers
	}
	if len(transfers) > 0 {
		transferQueue.In <- transfers
	}

	conf.RecordSuccess(e.Network, cast.ToString(job.To))
	lookbackTrack.done(e.Network, job.From, true)
	if !job.Lookback {
		cursorOf(e.Network).complete(job.From, job.To)
	}
	if job.JobID != 0 {
		scanJobPartDone(job.JobID)
	}

	log.Task.Info(fmt.Sprintf("区块扫描完成(%s): %d → %d 成功率：%s", e.Network, job.From, job.To, conf.GetSuccessRate(e.Network)))
}

// retryBlocks 退避后重新入队；重试耗尽则放弃并通知回溯追踪
func (e *evm) retryBlocks(job evmBlock, reason string, entry *logrus.Entry) {
	job.Attempt++
	if delay, ok := scanRetryLater(e.queueFor(job), job, job.Attempt); ok {
		entry.WithField("next_retry_in", delay.Round(time.Millisecond).String()).Warn("block scan failed, will retry: " + reason)

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
			Index:       int(utils.HexStr2Int(tx.Get("transactionIndex").String()).Int64()),
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

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer([]byte("["+strings.Join(items, ",")+"]")))
	if err != nil {

		return transfers, errors.Join(errors.New("eth_getLogs NewRequest Error"), err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := e.Client.Do(req)
	if err != nil {

		return transfers, errors.Join(errors.New("eth_getLogs Post Error"), err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {

		return transfers, errors.Join(errors.New("eth_getLogs ReadAll Error"), err)
	}

	if resp.StatusCode != 200 {

		return transfers, fmt.Errorf("eth_getLogs http status %d: %s", resp.StatusCode, bodySnippet(body))
	}

	data := gjson.ParseBytes(body)
	if !data.IsArray() {
		if rpcErr, ok := parseRpcError(data); ok {
			return transfers, fmt.Errorf("eth_getLogs rpc error code=%d message=%s", rpcErr.Code, rpcErr.Message)
		}

		return transfers, fmt.Errorf("eth_getLogs batch response is not an array: %s", bodySnippet(body))
	}

	byId := make(map[int64]gjson.Result)
	for _, itm := range data.Array() {
		byId[itm.Get("id").Int()] = itm
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
			to := log.Get("address").String()
			tradeType, ok := model.GetContractTrade(to)
			if !ok {

				continue
			}

			topics := log.Get("topics").Array()
			if len(topics) < 3 || topics[0].String() != evmTransferEvent { // transfer event signature

				continue
			}

			fromTopic, recvTopic := topics[1].String(), topics[2].String()
			if len(fromTopic) < 66 || len(recvTopic) < 66 {

				continue
			}

			dataHex := log.Get("data").String()
			if len(dataHex) <= 2 {

				continue
			}

			amount, ok := big.NewInt(0).SetString(dataHex[2:], 16)
			if !ok || amount.Sign() <= 0 {

				continue
			}

			if bh := log.Get("blockHash").String(); bh != "" && !strings.EqualFold(bh, ref.Hash) {
				// 返回的日志不属于请求的块：节点数据不一致，整批重试
				return transfers, fmt.Errorf("eth_getLogs block %d returned log of another block hash %s", i, bh)
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
				Index:       int(utils.HexStr2Int(log.Get("logIndex").String()).Int64()),
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
		post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":1}`, o.RefHash))
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(post))
		if err != nil {
			log.Task.Warn("evm tradeConfirmHandle Error creating request:", err)

			return
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err := e.Client.Do(req)
		if err != nil {
			log.Task.Warn("evm tradeConfirmHandle Error sending request:", err)
			e.rpc.failed(endpoint)

			return
		}

		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Task.Warn("evm tradeConfirmHandle Error reading response body:", err)

			return
		}

		data := gjson.ParseBytes(body)
		if data.Get("error").Exists() {
			log.Task.Warn(fmt.Sprintf("%s eth_getTransactionReceipt response error %s", e.Network, data.Get("error").String()))

			return
		}

		if data.Get("result.status").String() == "0x1" {
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

func (e *evm) rpcEndpoint() string {
	return e.rpc.current()
}
