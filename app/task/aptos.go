package task

import (
	"context"
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

type aptos struct {
	versionChunkSize       int
	versionConfirmedOffset int
	lastVersion            int
	versionQueue           *chanx.UnboundedChan[version] // 实时，高优先级
	lookbackQueue          *chanx.UnboundedChan[version] // 回溯 / 回放，低优先级
	client                 *http.Client
	rpc                    endpointPicker
}

// version 待扫描的交易版本区间、已失败次数、来源队列及所属持久化任务
type version struct {
	Start    int
	Limit    int
	Attempt  int
	Lookback bool
	JobID    int64
}

// aptosHeightTolerance Aptos 版本号增长很快，链头跳跃容忍度使用独立的更大值
const aptosHeightTolerance = 10000

var apt aptos

type aptEvent struct {
	Type    string
	Action  string
	Amount  decimal.Decimal
	Address string
}

type aptAmount struct {
	Amount string
	Type   model.TradeType
}

func init() {
	apt = newAptos()
	Register(Task{Callback: apt.versionDispatch})
	Register(Task{Callback: apt.syncVersionForward, Duration: time.Second * 3})
	Register(Task{Callback: apt.tradeConfirmHandle, Duration: time.Second * 5})
	Register(Task{Callback: apt.lookbackVersion, Duration: time.Second * 15})
	registerScanner(conf.Aptos, apt.status, apt.replay)
}

func newAptos() aptos {
	return aptos{
		versionChunkSize:       100,
		versionConfirmedOffset: 1000,
		lastVersion:            0,
		versionQueue:           chanx.NewUnboundedChan[version](context.Background(), 30),
		lookbackQueue:          chanx.NewUnboundedChan[version](context.Background(), 30),
		client:                 utils.NewHttpClient(),
		rpc:                    endpointPicker{network: conf.Aptos},
	}
}

// apiUrl 拼接 REST 路径，兼容配置末尾有无斜杠
func aptosApiUrl(endpoint, path string) string {
	return strings.TrimRight(endpoint, "/") + "/" + strings.TrimLeft(path, "/")
}

func (a *aptos) queueFor(p version) *chanx.UnboundedChan[version] {
	if p.Lookback {
		return a.lookbackQueue
	}

	return a.versionQueue
}

func (a *aptos) status() ScanStatus {
	return ScanStatus{
		Network:       conf.Aptos,
		HeadHeight:    int64(a.lastVersion),
		RealtimeQueue: a.versionQueue.Len(),
		LookbackQueue: a.lookbackQueue.Len(),
		Endpoint:      a.rpc.current(),
		Endpoints:     a.rpc.list(),
	}
}

// replay 回放 / 任务重试版本区间，按 chunk 切分进入低优先级队列
func (a *aptos) replay(from, to, jobID int64) int {
	n := 0
	for i := int(from); i <= int(to); i += a.versionChunkSize {
		limit := a.versionChunkSize
		if i+limit-1 > int(to) {
			limit = int(to) - i + 1
		}
		a.lookbackQueue.In <- version{Start: i, Limit: limit, Lookback: true, JobID: jobID}
		n++
	}

	return n
}

func (a *aptos) syncVersionForward(ctx context.Context) {
	if syncBreak(conf.Aptos, a.versionQueue.Len()) {

		return
	}

	endpoint := a.rpc.current()
	entry := scanLogger(conf.Aptos, endpoint, "ledger_info", nil)
	req, _ := http.NewRequestWithContext(ctx, "GET", aptosApiUrl(endpoint, "v1"), nil)
	resp, err := a.client.Do(req)
	if err != nil {
		entry.WithField("error", err.Error()).Warn("aptos syncVersionForward request error")
		a.rpc.failed(endpoint)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		entry.WithField("error", err.Error()).Warn("aptos syncVersionForward read body error")
		a.rpc.failed(endpoint)

		return
	}

	if resp.StatusCode != 200 {
		entry.WithFields(logrus.Fields{"http_status": resp.StatusCode, "body": bodySnippet(body)}).Warn("aptos syncVersionForward http status error")
		a.rpc.failed(endpoint)

		return
	}

	now := int(gjson.GetBytes(body, "ledger_version").Int())
	if now <= 0 {
		entry.WithField("body", bodySnippet(body)).Warn("aptos syncVersionForward invalid ledger_version")
		a.rpc.failed(endpoint)

		return
	}

	// 链头陈旧检测：ledger_timestamp 为微秒
	if headTime := time.UnixMicro(gjson.GetBytes(body, "ledger_timestamp").Int()); headIsStale(headTime) {
		entry.WithFields(logrus.Fields{"head": now, "head_time": headTime.Format(time.DateTime)}).Warn("stale head: node is behind, switching endpoint")
		a.rpc.failed(endpoint)

		return
	}

	// lastVersion 是下一个待扫描版本；resumeFrom 以"最后已发出"语义工作，故 -1 / +1 换算
	last := int64(a.lastVersion) - 1
	if a.lastVersion == 0 {
		last = 0
	}
	a.lastVersion = int(resumeFrom(conf.Aptos, last, int64(now), aptosHeightTolerance)) + 1
	if a.lastVersion >= now {

		return
	}

	cursorOf(conf.Aptos).issue(int64(a.lastVersion), int64(now)-1)

	var sub = now - a.lastVersion
	if sub <= a.versionChunkSize {
		a.versionQueue.In <- version{Start: a.lastVersion, Limit: sub}
	} else {
		chunks := (sub + a.versionChunkSize - 1) / a.versionChunkSize
		for i := 0; i < chunks; i++ {
			limit := a.versionChunkSize
			start := a.lastVersion + a.versionChunkSize*i
			if i == chunks-1 {
				limit = sub % a.versionChunkSize
				if limit == 0 {
					limit = a.versionChunkSize
				}
			}

			a.versionQueue.In <- version{Start: start, Limit: limit}
		}
	}

	a.lastVersion = now
}

func (a *aptos) lookbackVersion(ctx context.Context) {
	if a.lookbackQueue.Len() >= blockQueueLimit || !scanRequired(conf.Aptos) {
		return
	}

	startAt, endAt, ok := beginLookback(conf.Aptos)
	if !ok {
		return
	}

	start, end := blockapi.New().GetBoundaryHeights(startAt, endAt, conf.Aptos)
	if start <= 0 || end < start {
		log.Task.Warn(fmt.Sprintf("Aptos 回溯版本范围无效: start=%d end=%d", start, end))
		lookbackTrack.abort(conf.Aptos)

		return
	}

	for i := int(start); i <= int(end); i += a.versionChunkSize {
		if !waitQueueRoom(ctx, a.lookbackQueue) {
			lookbackTrack.abort(conf.Aptos)

			return
		}
		limit := a.versionChunkSize
		if i+limit > int(end) {
			limit = int(end) - i + 1
		}
		lookbackTrack.track(conf.Aptos, int64(i))
		a.lookbackQueue.In <- version{Start: i, Limit: limit, Lookback: true}
		time.Sleep(time.Millisecond * 200) // 速率控制
	}

	lookbackTrack.sealed(conf.Aptos)
}

func (a *aptos) versionDispatch(ctx context.Context) {
	p, err := ants.NewPoolWithFunc(3, a.versionParse)
	if err != nil {
		log.Task.Warn("aptos versionDispatch Error:", err)

		return
	}

	defer p.Release()

	for {
		job, ok := takeJob(ctx, a.versionQueue, a.lookbackQueue)
		if !ok {
			return
		}

		if err := p.Invoke(job); err != nil {
			a.queueFor(job).In <- job
			log.Task.Warn("versionDispatch Error invoking process slot:", err)
		}
	}
}

// 由于 aptos 网络特性，交易数据中不会显示存在交易转账 from => to 的对应关系，
// 所以目前此解析函数存在大量循环嵌套解析，逻辑较为复杂，希望未来有更好的方式进行解析 慢慢优化
func (a *aptos) versionParse(n any) {
	p := n.(version)

	var net = conf.Aptos
	var endpoint = a.rpc.current()
	var url = aptosApiUrl(endpoint, fmt.Sprintf("v1/transactions?start=%d&limit=%d", p.Start, p.Limit))

	entry := scanLogger(net, endpoint, "transactions", logrus.Fields{
		"start":        p.Start,
		"limit":        p.Limit,
		"attempt":      p.Attempt,
		"lookback":     p.Lookback,
		"queue_length": a.queueFor(p).Len(),
	})

	// retry 退避重试或放弃
	retry := func(reason string, fields logrus.Fields) {
		p.Attempt++
		if delay, ok := scanRetryLater(a.queueFor(p), p, p.Attempt); ok {
			entry.WithFields(fields).WithField("next_retry_in", delay.Round(time.Millisecond).String()).Warn("version scan failed, will retry: " + reason)

			return
		}

		scanAbandon(net, int64(p.Start), int64(p.Start+p.Limit-1), p.JobID, reason, entry.WithFields(fields))
	}

	// fail 节点类失败：切换节点并退避重试，成功计数放在解析完成之后
	fail := func(reason string, fields logrus.Fields) {
		conf.RecordFailure(net)
		a.rpc.failed(endpoint)
		retry(reason, fields)
	}

	// unavailable 数据尚未可用（节点落后，返回的交易不足一个分片）：前两次原节点等待，持续不可用才切换
	unavailable := func(reason string, fields logrus.Fields) {
		if p.Attempt > 0 {
			conf.RecordFailure(net)
		}
		if unavailableSwitch(p.Attempt) {
			a.rpc.failed(endpoint)
		}
		retry(reason, fields)
	}

	ctx, cancel := context.WithTimeout(context.Background(), scanRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		fail("create request error", logrus.Fields{"error": err.Error()})

		return
	}

	resp, err := a.client.Do(req)
	if err != nil {
		fail("http request error", logrus.Fields{"error": err.Error()})

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fail("read response body error", logrus.Fields{"http_status": resp.StatusCode, "error": err.Error()})

		return
	}

	if resp.StatusCode != 200 {
		fail("http status error", logrus.Fields{"http_status": resp.StatusCode, "body": bodySnippet(body)})

		return
	}

	if !gjson.ValidBytes(body) || !gjson.ParseBytes(body).IsArray() {
		fail("invalid json response", logrus.Fields{"body": bodySnippet(body)})

		return
	}

	arr := gjson.ParseBytes(body).Array()
	if len(arr) < p.Limit {
		// 落后节点只返回了部分交易：不能把整个区间当作扫描完成
		unavailable("partial chunk: ledger behind", logrus.Fields{"got": len(arr), "want": p.Limit})

		return
	}

	transfers := make([]transfer, 0)
	for _, trans := range arr {
		tsNano := trans.Get("timestamp").Int() * 1000
		timestamp := time.Unix(tsNano/1e9, tsNano%1e9)

		ver := int(trans.Get("version").Int())
		hash := trans.Get("hash").String()
		addrOwner := make(map[string]string)                                         // [address] => owner address
		addrType := make(map[string]model.TradeType)                                 // [address] => tradeType
		amtAddrMap := map[string]map[aptAmount]string{"deposit": {}, "withdraw": {}} // [amount] => address
		aptEvents := make([]aptEvent, 0)
		trans.Get("changes").ForEach(func(_, v gjson.Result) bool {
			if v.Get("type").String() != "write_resource" {

				return true
			}

			data := v.Get("data")
			if data.Get("type").String() == "0x1::fungible_asset::FungibleStore" {
				addr := v.Get("address").String()
				switch data.Get("data.metadata.inner").String() {
				case conf.UsdtAptos:
					addrType[addr] = model.UsdtAptos
				case conf.UsdcAptos:
					addrType[addr] = model.UsdcAptos
				}
			}
			if data.Get("type").String() == "0x1::object::ObjectCore" {
				addrOwner[v.Get("address").String()] = data.Get("data.owner").String()
			}

			return true
		})
		trans.Get("events").ForEach(func(_, v gjson.Result) bool {
			amount := v.Get("data.amount").String()
			amt, err := decimal.NewFromString(amount)
			if err != nil {

				return true
			}

			address := v.Get("data.store").String()
			switch v.Get("type").String() {
			case "0x1::fungible_asset::Deposit":
				aptEvents = append(aptEvents, aptEvent{Amount: amt, Address: address, Action: "deposit"})
				amtAddrMap["deposit"][aptAmount{Amount: amount, Type: addrType[address]}] = address
			case "0x1::fungible_asset::Withdraw":
				amtAddrMap["withdraw"][aptAmount{Amount: amount, Type: addrType[address]}] = address
				aptEvents = append(aptEvents, aptEvent{Amount: amt, Address: address, Action: "withdraw"})
			}
			return true
		})

		// 针对 一个withdraw 对应 一个deposit 且数额相同的情况
		for amt, to := range amtAddrMap["deposit"] {
			from, ok := amtAddrMap["withdraw"][amt]
			if !ok {

				continue
			}

			amount, ok := new(big.Int).SetString(amt.Amount, 10)
			if !ok {

				continue
			}

			tradeType, ok := addrType[to]
			if !ok {

				continue
			}

			transfers = append(transfers, transfer{
				Network:     net,
				TxHash:      hash,
				Amount:      decimal.NewFromBigInt(amount, model.GetTradeDecimal(tradeType)),
				FromAddress: a.padAddressLeadingZeros(addrOwner[from]),
				RecvAddress: a.padAddressLeadingZeros(addrOwner[to]),
				Timestamp:   timestamp,
				TradeType:   tradeType,
				BlockNum:    ver,
			})
		}

		// 针对 一个withdraw 对应 多个deposit(数额累计等于 withdraw) 的情况
		processEvents := func(tradeType model.TradeType, events []aptEvent) ([]aptEvent, map[string]string) {
			deposits := make([]aptEvent, 0)
			withdraws := make(map[decimal.Decimal]aptEvent)
			fromMap := make(map[string]string)

			// 分类事件
			for _, e := range events {
				if addrType[e.Address] == tradeType {
					if e.Action == "deposit" {
						deposits = append(deposits, e)
					}
					if e.Action == "withdraw" {
						withdraws[e.Amount] = e
					}
				}
			}

			// 穷举计算匹配关系，只穷举 A + B = C 的情况，实际上还存在 A + B + C + ... = D
			// 大部分这种情况都是合约 swap 等交易，非普通人1对1转账，所以选择忽视
			for k1, e1 := range deposits {
				for k2, e2 := range deposits {
					if k1 == k2 {
						continue
					}
					for sum, e3 := range withdraws {
						if e1.Amount.Add(e2.Amount).Equal(sum) {
							fromMap[e1.Address] = e3.Address
						}
					}
				}
			}

			return deposits, fromMap
		}
		generateTransfers := func(deposits []aptEvent, fromMap map[string]string, t model.TradeType, decimals int32) {
			for _, to := range deposits {
				if from, ok := fromMap[to.Address]; ok {
					transfers = append(transfers, transfer{
						Network:     net,
						TxHash:      hash,
						Amount:      decimal.NewFromBigInt(to.Amount.BigInt(), decimals),
						FromAddress: a.padAddressLeadingZeros(addrOwner[from]),
						RecvAddress: a.padAddressLeadingZeros(addrOwner[to.Address]),
						Timestamp:   timestamp,
						TradeType:   t,
						BlockNum:    ver,
					})
				}
			}
		}

		// 处理 USDT
		usdtDeposits, usdtFrom := processEvents(model.UsdtAptos, aptEvents)
		generateTransfers(usdtDeposits, usdtFrom, model.UsdtAptos, model.GetTradeDecimal(model.UsdtAptos))

		// 处理 USDC
		usdcDeposits, usdcFrom := processEvents(model.UsdcAptos, aptEvents)
		generateTransfers(usdcDeposits, usdcFrom, model.UsdcAptos, model.GetTradeDecimal(model.UsdcAptos))
	}

	if len(transfers) > 0 {

		transferQueue.In <- transfers
	}

	conf.RecordSuccess(net, cast.ToString(p.Start+p.Limit))
	lookbackTrack.done(net, int64(p.Start), true)
	if !p.Lookback {
		cursorOf(net).complete(int64(p.Start), int64(p.Start+p.Limit-1))
	}
	if p.JobID != 0 {
		scanJobPartDone(p.JobID)
	}

	log.Task.Info(fmt.Sprintf("区块扫描完成(Aptos) %d.%d 成功率：%s", p.Start, p.Limit, conf.GetSuccessRate(net)))
}

func (a *aptos) padAddressLeadingZeros(addr string) string {
	addr = strings.TrimPrefix(addr, "0x")
	addr = strings.Repeat("0", 64-len(addr)) + addr

	return "0x" + addr
}

func (a *aptos) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(model.GetNetworkTrades(conf.Aptos))
	var wg sync.WaitGroup

	var handle = func(o model.Order) {
		if model.GetC(model.BlockOffsetConfirm) == "1" {
			if a.lastVersion == 0 {
				return
			}
			if a.lastVersion-o.RefBlockNum < a.versionConfirmedOffset {
				return
			}
		}

		endpoint := a.rpc.current()
		req, _ := http.NewRequestWithContext(ctx, "GET", aptosApiUrl(endpoint, "v1/transactions/by_hash/"+o.RefHash), nil)
		resp, err := a.client.Do(req)
		if err != nil {
			log.Task.Warn("aptos tradeConfirmHandle Error sending request:", err)
			a.rpc.failed(endpoint)

			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			log.Task.Warn("aptos tradeConfirmHandle Error response status code:", resp.StatusCode)
			a.rpc.failed(endpoint)

			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Task.Warn("aptos tradeConfirmHandle Error reading response body:", err)

			return
		}

		data := gjson.ParseBytes(body)
		if data.Get("error_code").Exists() {
			log.Task.Warn("aptos tradeConfirmHandle Error:", data.Get("message").String())

			return
		}

		if data.Get("version").String() != "" &&
			data.Get("success").Bool() &&
			data.Get("vm_status").String() == "Executed successfully" {

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
