package task

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/base58"
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

// 参考文档
//  - https://solana.com/zh/docs/rpc
//  - https://github.com/solana-program/token/blob/6d18ff73b1dd30703a30b1ca941cb0f1d18c2b2a/program/src/instruction.rs

// solanaMaxTxVersion getBlock 请求的 maxSupportedTransactionVersion。
// 区块中只要存在一笔高于此版本的交易，整个 getBlock 就会返回 -32015，导致该 slot 内所有转账漏扫。
// 每提升一个版本都需确认下方解析逻辑兼容（accountKeys + meta.loadedAddresses 的账户索引方式）。
const solanaMaxTxVersion = 1

// solanaScanWorkers Solana 区块解析默认并发数
const solanaScanWorkers = 10

type solana struct {
	slotConfirmedOffset int
	lastSlotNum         int
	slotQueue           *chanx.UnboundedChan[solanaSlot] // 实时区块，高优先级
	lookbackQueue       *chanx.UnboundedChan[solanaSlot] // 订单回溯 / 手动回放，低优先级
	client              *http.Client
	rpc                 endpointPicker
}

// solanaSlot 待扫描 slot、已失败次数、来源队列及所属持久化任务
type solanaSlot struct {
	Slot     int
	Attempt  int
	Lookback bool
	JobID    int64
}

type solanaTokenOwner struct {
	TradeType model.TradeType
	Address   string
}

var sol solana

func init() {
	sol = newSolana()
	Register(Task{Callback: sol.slotDispatch})
	Register(Task{Callback: sol.syncSlotForward, Duration: time.Second * 5})
	Register(Task{Callback: sol.tradeConfirmHandle, Duration: time.Second * 5})
	Register(Task{Callback: sol.lookbackSlots, Duration: time.Second * 15})
	registerScanner(conf.Solana, sol.status, sol.replay)
}

func newSolana() solana {
	return solana{
		slotConfirmedOffset: 60,
		lastSlotNum:         0,
		slotQueue:           chanx.NewUnboundedChan[solanaSlot](context.Background(), 30),
		lookbackQueue:       chanx.NewUnboundedChan[solanaSlot](context.Background(), 30),
		client:              utils.NewHttpClient(),
		rpc:                 endpointPicker{network: conf.Solana},
	}
}

func (s *solana) rpcEndpoint() string {
	return s.rpc.current()
}

// queueFor 任务所属队列：重试必须回到原队列，回溯任务不能挤占实时队列
func (s *solana) queueFor(job solanaSlot) *chanx.UnboundedChan[solanaSlot] {
	if job.Lookback {
		return s.lookbackQueue
	}

	return s.slotQueue
}

func (s *solana) status() ScanStatus {
	return ScanStatus{
		Network:       conf.Solana,
		HeadHeight:    int64(s.lastSlotNum),
		RealtimeQueue: s.slotQueue.Len(),
		LookbackQueue: s.lookbackQueue.Len(),
		Endpoint:      s.rpc.current(),
		Endpoints:     s.rpc.list(),
	}
}

// replay 回放 / 任务重试：进入低优先级队列，不影响实时扫描
func (s *solana) replay(from, to, jobID int64) int {
	n := 0
	for i := from; i <= to; i++ {
		s.lookbackQueue.In <- solanaSlot{Slot: int(i), Lookback: true, JobID: jobID}
		n++
	}

	return n
}

func (s *solana) syncSlotForward(ctx context.Context) {
	if syncBreak(conf.Solana, s.slotQueue.Len()) {

		return
	}

	endpoint := s.rpcEndpoint()
	entry := scanLogger(conf.Solana, endpoint, "getSlot", nil)
	req, _ := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer([]byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		entry.WithField("error", err.Error()).Warn("syncSlotForward request error")
		s.rpc.failed(endpoint)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		entry.WithField("error", err.Error()).Warn("syncSlotForward read body error")
		s.rpc.failed(endpoint)

		return
	}

	if resp.StatusCode != 200 {
		entry.WithFields(logrus.Fields{"http_status": resp.StatusCode, "body": bodySnippet(body)}).Warn("syncSlotForward http status error")
		s.rpc.failed(endpoint)

		return
	}

	res := gjson.ParseBytes(body)
	if rpcErr, ok := parseRpcError(res); ok {
		entry.WithFields(rpcErr.fields()).Warn("syncSlotForward rpc error")
		s.rpc.failed(endpoint)

		return
	}

	now := int(res.Get("result").Int())
	if now <= 0 {
		entry.WithField("body", bodySnippet(body)).Warn("syncSlotForward invalid slot number")
		s.rpc.failed(endpoint)

		return
	}

	// 链头陈旧检测：落后 / 停止同步的节点不能作为下发依据
	if headTime, ok := s.blockTime(ctx, endpoint, now); ok && headIsStale(headTime) {
		entry.WithFields(logrus.Fields{"head": now, "head_time": headTime.Format(time.DateTime)}).Warn("stale head: node is behind, switching endpoint")
		s.rpc.failed(endpoint)

		return
	}

	// 启动时从持久化游标续扫；链头跳跃超出容忍度时记录 gap 任务后对齐链头
	s.lastSlotNum = int(resumeFrom(conf.Solana, int64(s.lastSlotNum), int64(now), blockHeightTolerance()))

	if now <= s.lastSlotNum { // 区块高度没有变化

		return
	}

	cursorOf(conf.Solana).issue(int64(s.lastSlotNum)+1, int64(now))
	for n := s.lastSlotNum + 1; n <= now; n++ {
		// 待扫描区块入列

		s.slotQueue.In <- solanaSlot{Slot: n}
	}

	s.lastSlotNum = now
}

// blockTime 查询 slot 的区块时间；失败或 slot 被跳过时返回 false（不影响主流程）
func (s *solana) blockTime(ctx context.Context, endpoint string, slot int) (time.Time, bool) {
	post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getBlockTime","params":[%d]}`, slot))
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(post))
	if err != nil {
		return time.Time{}, false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return time.Time{}, false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		return time.Time{}, false
	}

	result := gjson.GetBytes(body, "result")
	if !result.Exists() || result.Type == gjson.Null || result.Int() <= 0 {
		return time.Time{}, false
	}

	return time.Unix(result.Int(), 0), true
}

func (s *solana) slotDispatch(ctx context.Context) {
	// Solana 约每秒 2.5 个 slot，单次 getBlock 在公共节点常需 1~2 秒，
	// 并发过低会直接追不上出块速度，这里默认给到 10
	p, err := ants.NewPoolWithFunc(scanWorkers(solanaScanWorkers), s.slotParse)
	if err != nil {
		log.Task.Warn("Error creating pool:", err)

		return
	}

	defer p.Release()

	for {
		job, ok := takeJob(ctx, s.slotQueue, s.lookbackQueue)
		if !ok {
			return
		}

		if err := p.Invoke(job); err != nil {
			s.queueFor(job).In <- job
			log.Task.Warn("slotDispatch Error invoking process slot:", err)
		}
	}
}

func (s *solana) slotParse(n any) {
	s.scanSlot(n.(solanaSlot))
}

// scanSlot 拉取并解析单个 slot。只有在 HTTP 200、JSON 合法、无 RPC error、result 非空（或确认为被跳过的 slot）、
// 全部交易解析完成并送入队列后才计为成功；其余任何路径都进入退避重试。
func (s *solana) scanSlot(job solanaSlot) {
	slot := job.Slot
	network := conf.Solana
	endpoint := s.rpc.current()
	entry := scanLogger(network, endpoint, "getBlock", logrus.Fields{
		"slot":         slot,
		"attempt":      job.Attempt,
		"lookback":     job.Lookback,
		"queue_length": s.queueFor(job).Len(),
	})

	// fail 记失败并安排重试；switchEndpoint 为 true 表示失败源于节点本身（网络/限流/非法响应），需要切换备用节点
	fail := func(reason string, fields logrus.Fields, switchEndpoint bool) {
		conf.RecordFailure(network)
		if switchEndpoint {
			s.rpc.failed(endpoint)
		}
		s.retrySlot(job, reason, entry.WithFields(fields))
	}

	// unavailable slot 尚未可用（未确认 / 节点落后）：前两次在原节点等待，持续不可用才切换；首次等待不计入失败率
	unavailable := func(reason string, fields logrus.Fields) {
		if job.Attempt > 0 {
			conf.RecordFailure(network)
		}
		if unavailableSwitch(job.Attempt) {
			s.rpc.failed(endpoint)
		}
		s.retrySlot(job, reason, entry.WithFields(fields))
	}

	ctx, cancel := context.WithTimeout(context.Background(), scanRequestTimeout)
	defer cancel()

	post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[%d,{"encoding":"json","maxSupportedTransactionVersion":%d,"transactionDetails":"full","rewards":false}]}`, slot, solanaMaxTxVersion))
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(post))
	if err != nil {
		fail("create request error", logrus.Fields{"error": err.Error()}, false)

		return
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
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
	if !res.IsObject() {
		fail("invalid json response", logrus.Fields{"http_status": resp.StatusCode, "body": bodySnippet(body)}, true)

		return
	}

	if rpcErr, ok := parseRpcError(res); ok {
		switch rpcErr.Code {
		case solRpcSlotSkippedLedger, solRpcSlotSkippedStorage:
			// 被跳过的 slot 不含任何交易，视为空区块扫描完成
			s.slotDone(job)
			entry.WithFields(rpcErr.fields()).Info("slot skipped, treated as empty block")

			return
		case solRpcBlockNotAvailable:
			// 区块尚未确认，属于时序问题而非节点故障
			unavailable("block not available yet", rpcErr.fields())

			return
		case solRpcUnsupportedTxVer:
			entry.WithFields(rpcErr.fields()).Error(fmt.Sprintf("transaction version unsupported, solanaMaxTxVersion=%d needs upgrade", solanaMaxTxVersion))
		case solRpcBlockCleanedUp:
			entry.WithFields(rpcErr.fields()).Error("block cleaned up by node, an archive RPC is required")
		}

		// -32015 版本不支持、-32001 已清理及其它未知错误：均不能计为成功，切换节点后重试
		fail("rpc error", rpcErr.fields(), true)

		return
	}

	result := res.Get("result")
	if !result.Exists() || result.Type == gjson.Null {
		// 节点暂时没有该 slot 数据（落后）：原节点等待，持续不可用才切换
		unavailable("result is null (node behind)", nil)

		return
	}

	timestamp := time.Unix(result.Get("blockTime").Int(), 0)

	for _, trans := range result.Get("transactions").Array() {
		// 交易内转账序号：外层指令用其位置，内层指令用 1000 + 外层位置*100 + 内层位置
		// 执行失败的交易，其指令并未生效，跳过以免把失败转账误认为真实入账
		if e := trans.Get("meta.err"); e.Exists() && e.Type != gjson.Null {

			continue
		}

		hash := trans.Get("transaction.signatures.0").String()

		// 解析账号索引
		accountKeys := make([]string, 0)
		for _, key := range trans.Get("transaction.message.accountKeys").Array() {
			accountKeys = append(accountKeys, key.String())
		}
		for _, v := range []string{"readonly", "writable"} {
			for _, key := range trans.Get("meta.loadedAddresses." + v).Array() {
				accountKeys = append(accountKeys, key.String())
			}
		}

		// 查找SPL Token索引
		splTokenIndex := int64(-1)
		for i, v := range accountKeys {
			if v == conf.SolSplToken {
				splTokenIndex = int64(i)

				break
			}
		}

		// SPL Token的Mint地址，即不包含 Token 交易信息
		if splTokenIndex == -1 {

			continue
		}

		// 解析 Token 账户 【Token Wallet => Owner Wallet】
		tokenAccountMap := make(map[string]solanaTokenOwner)
		for _, v := range []string{"postTokenBalances", "preTokenBalances"} {
			for _, itm := range trans.Get("meta." + v).Array() {
				tradeType, ok := model.GetContractTrade(itm.Get("mint").String())
				if !ok || itm.Get("programId").String() != conf.SolSplToken {

					continue
				}

				idx := itm.Get("accountIndex").Int()
				if idx < 0 || idx >= int64(len(accountKeys)) {

					continue
				}

				tokenAccountMap[accountKeys[idx]] = solanaTokenOwner{
					TradeType: tradeType,
					Address:   itm.Get("owner").String(),
				}
			}
		}

		transArr := make([]transfer, 0)

		// 解析外部指令
		for i, instr := range trans.Get("transaction.message.instructions").Array() {
			if instr.Get("programIdIndex").Int() != splTokenIndex {

				continue
			}

			t := s.parseTransfer(instr, accountKeys, tokenAccountMap)
			t.Index = i
			transArr = append(transArr, t)
		}

		// 解析内部指令
		for _, itm := range trans.Get("meta.innerInstructions").Array() {
			outer := int(itm.Get("index").Int())
			for j, instr := range itm.Get("instructions").Array() {
				if instr.Get("programIdIndex").Int() != splTokenIndex {

					continue
				}

				t := s.parseTransfer(instr, accountKeys, tokenAccountMap)
				t.Index = 1000 + outer*100 + j
				transArr = append(transArr, t)
			}
		}

		// 过滤无关交易
		result := make([]transfer, 0)
		for _, t := range transArr {
			if t.FromAddress == "" || t.RecvAddress == "" || t.Amount.IsZero() {

				continue
			}

			t.TxHash = hash
			t.Network = conf.Solana
			t.BlockNum = slot
			t.Timestamp = timestamp

			result = append(result, t)
		}

		if len(result) > 0 {
			transferQueue.In <- result
		}
	}

	s.slotDone(job)

	log.Task.Info(fmt.Sprintf("区块扫描完成(Solana) %d 成功率：%s", slot, conf.GetSuccessRate(network)))
}

// slotDone 成功收尾：成功计数、回溯结算、连续游标推进、任务进度
func (s *solana) slotDone(job solanaSlot) {
	conf.RecordSuccess(conf.Solana, cast.ToString(job.Slot))
	lookbackTrack.done(conf.Solana, int64(job.Slot), true)
	if !job.Lookback {
		cursorOf(conf.Solana).complete(int64(job.Slot), int64(job.Slot))
	}
	if job.JobID != 0 {
		scanJobPartDone(job.JobID)
	}
}

// retrySlot 退避后重新入队；重试耗尽则放弃并通知回溯追踪
func (s *solana) retrySlot(job solanaSlot, reason string, entry *logrus.Entry) {
	job.Attempt++
	if delay, ok := scanRetryLater(s.queueFor(job), job, job.Attempt); ok {
		entry.WithField("next_retry_in", delay.Round(time.Millisecond).String()).Warn("slot scan failed, will retry: " + reason)

		return
	}

	scanAbandon(conf.Solana, int64(job.Slot), int64(job.Slot), job.JobID, reason, entry)
}

func (s *solana) parseTransfer(instr gjson.Result, accountKeys []string, tokenAccountMap map[string]solanaTokenOwner) transfer {
	accounts := instr.Get("accounts").Array()
	trans := transfer{}
	if len(accounts) < 3 { // from to singer，至少存在3个账户索引，如果是多签则 > 3

		return trans
	}

	data := base58.Decode(instr.Get("data").String())
	dLen := len(data)
	if dLen < 9 {

		return trans
	}

	isTransfer := data[0] == 3 && dLen == 9
	isTransferChecked := data[0] == 12 && dLen == 10
	if !isTransfer && !isTransferChecked {

		return trans
	}

	var exp int32 = -6
	if isTransferChecked {
		exp = int32(data[9]) * -1
	}

	keyAt := func(i int) string {
		idx := accounts[i].Int()
		if idx < 0 || idx >= int64(len(accountKeys)) {
			return ""
		}

		return accountKeys[idx]
	}

	from, ok := tokenAccountMap[keyAt(0)]
	if !ok {

		return trans
	}

	trans.FromAddress = from.Address
	trans.RecvAddress = tokenAccountMap[keyAt(1)].Address
	if isTransferChecked {
		trans.RecvAddress = tokenAccountMap[keyAt(2)].Address
	}

	buf := make([]byte, 8)
	copy(buf[:], data[1:9])
	number := binary.LittleEndian.Uint64(buf)
	b := new(big.Int)
	b.SetUint64(number)
	trans.TradeType = from.TradeType
	trans.Amount = decimal.NewFromBigInt(b, exp)

	return trans
}

func (s *solana) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(model.GetNetworkTrades(conf.Solana))
	var wg sync.WaitGroup

	var handle = func(o model.Order) {
		if model.GetC(model.BlockOffsetConfirm) == "1" {
			if s.lastSlotNum == 0 {
				return
			}
			if s.lastSlotNum-o.RefBlockNum < s.slotConfirmedOffset {
				return
			}
		}

		endpoint := s.rpcEndpoint()
		post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["%s"],{"searchTransactionHistory":true}]}`, o.RefHash))
		req, _ := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(post))
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err != nil {
			log.Task.Warn("solana tradeConfirmHandle Error sending request:", err)
			s.rpc.failed(endpoint)

			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			log.Task.Warn("solana tradeConfirmHandle Error response status code:", resp.StatusCode)
			s.rpc.failed(endpoint)

			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Task.Warn("solana tradeConfirmHandle Error reading response body:", err)

			return
		}

		data := gjson.ParseBytes(body)
		if data.Get("error").Exists() {
			log.Task.Warn("solana tradeConfirmHandle Error:", data.Get("error").String())

			return
		}

		if data.Get("result.value.0.confirmationStatus").String() == "finalized" {

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

func (s *solana) lookbackSlots(ctx context.Context) {
	if s.lookbackQueue.Len() >= blockQueueLimit || !scanRequired(conf.Solana) {
		return
	}

	startAt, endAt, ok := beginLookback(conf.Solana)
	if !ok {
		return
	}

	start, end := blockapi.New().GetBoundaryHeights(startAt, endAt, conf.Solana)
	if start <= 0 || end < start {
		log.Task.Warn(fmt.Sprintf("Solana 回溯高度范围无效: start=%d end=%d", start, end))
		lookbackTrack.abort(conf.Solana)

		return
	}

	for i := int(start); i <= int(end); i++ {
		// 回溯走独立低优先级队列，拥堵时等待腾出空间，不再中断任务
		if !waitQueueRoom(ctx, s.lookbackQueue) {
			lookbackTrack.abort(conf.Solana)

			return
		}
		lookbackTrack.track(conf.Solana, int64(i))
		s.lookbackQueue.In <- solanaSlot{Slot: i, Lookback: true}
		time.Sleep(time.Millisecond * 200)
	}

	lookbackTrack.sealed(conf.Solana)
}
