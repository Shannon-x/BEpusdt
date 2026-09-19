package task

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/smallnest/chanx"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/notifier"
	"github.com/v03413/go-cache"
)

// 区块扫描公共设施：请求超时、失败重试（指数退避）、JSON-RPC 错误解析、结构化日志、回溯任务追踪。
// 目标是让"扫描成功"严格等价于"区块数据完整拿到、解析完成、转账已送入处理队列"，
// 任何失败路径都进入重试，而不是静默丢块。

const (
	scanRequestTimeout   = time.Second * 15 // 单次 RPC 请求超时
	scanRetryMaxAttempts = 10               // 单个区块最大重试次数，超过后放弃并输出 Error 日志
	scanRetryMaxDelay    = time.Minute      // 退避上限
	scanRetryJitter      = 0.25             // 退避随机抖动比例 ±25%
	scanBodySnippetLen   = 256              // 日志中响应体截断长度
)

// scanRetryBaseDelay 首次重试延迟；变量而非常量，便于测试缩短
var scanRetryBaseDelay = time.Second

// Solana JSON-RPC 错误码，参考 https://github.com/anza-xyz/agave/blob/master/rpc-client-api/src/custom_error.rs
const (
	solRpcBlockCleanedUp     = -32001 // 区块已被节点清理，需要 archive 节点
	solRpcBlockNotAvailable  = -32004 // 区块尚未可用：未确认或节点暂无数据，稍后重试
	solRpcSlotSkippedLedger  = -32007 // slot 被跳过，或因快照跳转缺失
	solRpcSlotSkippedStorage = -32009 // slot 被跳过，或在长期存储中缺失
	solRpcUnsupportedTxVer   = -32015 // 交易版本不受支持，maxSupportedTransactionVersion 过低
)

type rpcError struct {
	Code    int64
	Message string
}

// parseRpcError 解析 JSON-RPC 响应中的 error 字段
func parseRpcError(res gjson.Result) (rpcError, bool) {
	e := res.Get("error")
	if !e.Exists() || e.Type == gjson.Null {
		return rpcError{}, false
	}

	return rpcError{Code: e.Get("code").Int(), Message: e.Get("message").String()}, true
}

func (e rpcError) fields() logrus.Fields {
	return logrus.Fields{"rpc_error_code": e.Code, "rpc_error_message": e.Message}
}

// scanRetryDelay 计算第 attempt 次重试的退避延迟：base * 2^(attempt-1)，上限 scanRetryMaxDelay，附带随机抖动
func scanRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := scanRetryBaseDelay
	for i := 1; i < attempt && delay < scanRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > scanRetryMaxDelay {
		delay = scanRetryMaxDelay
	}

	jitter := 1 + (rand.Float64()*2-1)*scanRetryJitter

	return time.Duration(float64(delay) * jitter)
}

// scanRetryLater 按退避延迟后将任务重新入队。attempt 为已失败次数；达到上限返回 false 表示放弃。
// 延迟通过定时器实现，不阻塞消费协程，也避免立即回塞造成 RPC 风暴。
func scanRetryLater[T any](q *chanx.UnboundedChan[T], job T, attempt int) (time.Duration, bool) {
	if attempt >= scanRetryMaxAttempts {
		return 0, false
	}

	delay := scanRetryDelay(attempt)
	time.AfterFunc(delay, func() {
		q.In <- job
	})

	return delay, true
}

// providerHost 节点地址脱敏为主机名，避免 URL 中的 API Key 进入日志或告警
func providerHost(endpoint string) string {
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		return u.Host
	}

	return endpoint
}

// scanLogger 构造带统一字段的日志条目：network / provider / method
func scanLogger(network, endpoint, method string, fields logrus.Fields) *logrus.Entry {
	entry := log.Task.WithFields(logrus.Fields{
		"network":  network,
		"provider": providerHost(endpoint),
		"method":   method,
	})

	if len(fields) > 0 {
		entry = entry.WithFields(fields)
	}

	return entry
}

func bodySnippet(body []byte) string {
	if len(body) > scanBodySnippetLen {
		return string(body[:scanBodySnippetLen]) + "..."
	}

	return string(body)
}

// ---- 回溯任务追踪 ----
//
// 订单回溯每个订单只需成功完成一次。此前的实现在区块入队前就把订单标记为"已回溯"，
// 一旦后续 RPC 失败，该订单在进程存活期间不会再被回溯。这里改为：
//   1. begin：订单进入 inflight，避免下个周期重复入队；
//   2. track：每个入队的区块登记为 pending；
//   3. done：区块扫描结束（成功/放弃）时销账；
//   4. sealed / abort：全部区块入队完毕 / 入队中断；
// 只有全部区块成功才标记 done；任何区块放弃或入队中断，订单回到待回溯状态，等待下个周期重试。
// 注意：当前状态仅保存在内存中，进程重启后会对存活订单重新回溯一次（宁多勿漏）。

type lookbackState int

const (
	lookbackInflight lookbackState = iota + 1
	lookbackDone
)

type lookbackJob struct {
	orderIDs []int64
	pending  map[int64]struct{}
	sealed   bool
	failed   bool
}

type lookbackTracker struct {
	mu    sync.Mutex
	jobs  map[string]*lookbackJob // network → 进行中的回溯任务
	state sync.Map                // orderID(int64) → lookbackState
}

var lookbackTrack = newLookbackTracker()

func newLookbackTracker() *lookbackTracker {
	return &lookbackTracker{jobs: make(map[string]*lookbackJob)}
}

// begin 开启一个网络的回溯任务；若该网络已有任务在进行，返回 false
func (t *lookbackTracker) begin(network string, orderIDs []int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.jobs[network]; ok {
		return false
	}

	t.jobs[network] = &lookbackJob{orderIDs: orderIDs, pending: make(map[int64]struct{})}
	for _, id := range orderIDs {
		t.state.Store(id, lookbackInflight)
	}

	return true
}

// track 登记一个已入队的区块键（EVM 用批次起始高度，其它链用区块号/slot）
func (t *lookbackTracker) track(network string, key int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if job, ok := t.jobs[network]; ok {
		job.pending[key] = struct{}{}
	}
}

// sealed 声明全部区块已入队，之后 pending 清空即可结算
func (t *lookbackTracker) sealed(network string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if job, ok := t.jobs[network]; ok {
		job.sealed = true
		t.settle(network, job)
	}
}

// abort 入队过程被中断（队列拥堵、上下文取消、高度范围无效等），任务按失败结算
func (t *lookbackTracker) abort(network string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if job, ok := t.jobs[network]; ok {
		job.failed = true
		job.sealed = true
		t.settle(network, job)
	}
}

// done 区块扫描结束；ok=false 表示重试耗尽已放弃。非本任务的区块键会被忽略。
func (t *lookbackTracker) done(network string, key int64, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	job, exists := t.jobs[network]
	if !exists {
		return
	}
	if _, tracked := job.pending[key]; !tracked {
		return
	}

	delete(job.pending, key)
	if !ok {
		job.failed = true
	}

	t.settle(network, job)
}

// settle 需在持有锁时调用
func (t *lookbackTracker) settle(network string, job *lookbackJob) {
	if !job.sealed || len(job.pending) > 0 {
		return
	}

	delete(t.jobs, network)
	for _, id := range job.orderIDs {
		if job.failed {
			t.state.Delete(id)
		} else {
			t.state.Store(id, lookbackDone)
		}
	}
}

// markDone 直接标记订单回溯完成（同步扫描、无需区块级追踪的链使用）
func (t *lookbackTracker) markDone(orderIDs []int64) {
	for _, id := range orderIDs {
		t.state.Store(id, lookbackDone)
	}
}

// skip 订单是否无需再回溯（已完成或正在进行）
func (t *lookbackTracker) skip(orderID int64) bool {
	_, ok := t.state.Load(orderID)

	return ok
}

// inflight 网络是否有回溯任务在进行
func (t *lookbackTracker) inflight(network string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.jobs[network]

	return ok
}

// ---- 多节点高度差 ----
//
// 同一条链配置多个节点（或单个负载均衡节点池）时，各节点高度可能不一致：
//   - 链头只允许前进：落后节点报告的更低链头直接忽略，不回退、不重复下发；
//   - 链头陈旧检测：头部同步时校验最新区块的时间戳，落后超过 staleHeadAfter 的节点视为停止同步，切换并跳过本轮；
//   - "区块尚未可用"（EVM 返回 null、Solana -32004、Aptos 分片不足、Tron 空块）是时序问题：前两次在原节点等待，
//     持续不可用才切换节点；首次等待不计入失败率；
//   - EVM 日志按 blockHash 查询而不是按区间：保证日志与已校验的区块来自同一条链上的同一个块，节点没有该块会明确报错进入重试，
//     不会因为节点池里某个节点落后而静默漏掉日志。

const staleHeadAfter = 3 * time.Minute

// headIsStale 链头区块时间明显落后于当前时间，说明节点落后或停止同步，不应以它为准下发区块
func headIsStale(blockTime time.Time) bool {
	return !blockTime.IsZero() && blockTime.Unix() > 0 && time.Since(blockTime) > staleHeadAfter
}

// unavailableSwitch 区块尚未可用时是否切换节点：前两次在原节点等待，持续不可用才切换
func unavailableSwitch(attempt int) bool {
	return attempt >= 2
}

// ---- RPC 节点：主备切换 ----
//
// rpc_endpoint_* 配置支持多个节点（逗号/空白分隔），首个为主节点。
// 节点游标 rotate 指向当前使用的节点，所有请求（头部同步、区块扫描、交易确认）都走当前节点；
// 请求因节点原因失败（网络错误、非 200、非法响应、RPC error、数据缺失）时调用 failed 切换到下一个，
// 之后的区块重试与新区块自然落在新节点上。
// 游标不会自动切回主节点：只要当前节点可用就一直使用，直到它再次失败。

type endpointPicker struct {
	network  model.Network
	override []string // 非空时覆盖配置，测试用
	mu       sync.Mutex
	rotate   int
}

func (p *endpointPicker) list() []string {
	if len(p.override) > 0 {
		return p.override
	}

	return model.Endpoints(p.network)
}

// current 当前节点
func (p *endpointPicker) current() string {
	list := p.list()
	if len(list) == 0 {
		return ""
	}

	p.mu.Lock()
	idx := p.rotate % len(list)
	p.mu.Unlock()

	return list[idx]
}

// failed 声明某节点请求失败；仅当它仍是当前节点时才切换，避免多个并发 worker 同时失败把游标推过头
func (p *endpointPicker) failed(endpoint string) {
	list := p.list()
	if len(list) <= 1 || endpoint == "" {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if list[p.rotate%len(list)] != endpoint {
		return
	}

	p.rotate = (p.rotate + 1) % len(list)
	scanLogger(string(p.network), endpoint, "", logrus.Fields{"next_provider": providerHost(list[p.rotate])}).Warn("rpc endpoint failed, switched to next")
}

// ---- 告警 ----

// alertSender 告警发送函数，测试可替换
var alertSender = notifier.Alert

// scanAlert 发送限频告警：同一 key 在 every 周期内只发一次
func scanAlert(key string, every time.Duration, title, text string) {
	k := "scan_alert_" + key
	if _, ok := cache.Get(k); ok {
		return
	}

	cache.Set(k, true, every)
	log.Task.Warn(fmt.Sprintf("[ALERT] %s：%s", title, strings.ReplaceAll(text, "\n", " ")))
	alertSender(title, text)
}

// ---- 放弃统计 ----

var scanAbandonCount sync.Map // network → *atomic.Int64

// scanAbandon 区块重试耗尽后的统一处理：
//  1. 结算回溯任务（订单回到待回溯）；
//  2. 持久化：属于某个任务的区块 → 记该任务失败；否则新建 abandoned 任务等待自动重试；
//  3. 区块已落库为任务后允许连续游标越过它，游标不会因单个坏块永远停滞；
//  4. Error 日志 + 限频告警。
func scanAbandon(network string, from, to, jobID int64, reason string, entry *logrus.Entry) {
	v, _ := scanAbandonCount.LoadOrStore(network, new(atomic.Int64))
	total := v.(*atomic.Int64).Add(1)

	lookbackTrack.done(network, from, false)

	if jobID != 0 {
		scanJobPartFailed(jobID, reason)
	} else if _, err := model.CreateScanJob(network, from, to, model.ScanJobKindAbandoned, model.ScanJobStatusPending, reason, time.Now().Add(model.ScanJobRetryBase)); err != nil {
		entry.WithField("error", err.Error()).Error("persist abandoned block failed")
	}

	cursorOf(network).complete(from, to)
	entry.WithField("abandoned_total", total).Error("scan abandoned after max attempts: " + reason)

	scanAlert("abandon_"+network, 5*time.Minute, "区块扫描放弃",
		fmt.Sprintf("网络：%s\n区块：%d → %d\n原因：%s\n已重试 %d 次仍失败，本进程累计放弃 %d 个区块。\n已写入失败任务表，将在 %s 后自动重试；也可通过后台 POST /api/scan/replay 立即补扫。",
			network, from, to, reason, scanRetryMaxAttempts, total, model.ScanJobRetryBase))
}

func abandonedBlocks(network string) int64 {
	if v, ok := scanAbandonCount.Load(network); ok {
		return v.(*atomic.Int64).Load()
	}

	return 0
}

// ---- 扫描器注册：状态查询与区块回放 ----

type ScanStatus struct {
	Network         string           `json:"network"`
	HeadHeight      int64            `json:"head_height"`      // 本地已知的链最新高度
	LastBlock       string           `json:"last_block"`       // 最近一次成功扫描的区块
	LastSuccessAt   int64            `json:"last_success_at"`  // 最近一次成功扫描的时间戳，0 表示启动后尚未成功
	SuccessRate     string           `json:"success_rate"`     // 最近 1000 次扫描成功率
	RealtimeQueue   int              `json:"realtime_queue"`   // 实时区块待扫描数量
	LookbackQueue   int              `json:"lookback_queue"`   // 回溯/回放区块待扫描数量
	LookbackActive  bool             `json:"lookback_active"`  // 是否有回溯任务在进行
	AbandonedBlocks int64            `json:"abandoned_blocks"` // 本进程累计放弃的区块数
	CursorHeight    int64            `json:"cursor_height"`    // 已连续扫描完成并持久化的高度
	Jobs            map[string]int64 `json:"jobs"`             // 任务表各状态数量
	Endpoint        string           `json:"endpoint"`         // 当前使用的节点
	Endpoints       []string         `json:"endpoints"`        // 配置的全部节点
}

type scanHandle struct {
	status func() ScanStatus
	replay func(from, to, jobID int64) int // 把区间送入低优先级队列，jobID 用于回报任务完成情况
}

var scanRegistry sync.Map // network → scanHandle

const replayMaxBlocks = 5000 // 单次回放区块数上限，防止误操作把队列打满

func registerScanner(network string, status func() ScanStatus, replay func(from, to, jobID int64) int) {
	scanRegistry.Store(network, scanHandle{status: status, replay: replay})
}

// fillScanStatus 补齐各链通用字段；jobs 为该网络各状态任务数（由调用方一次查出全部网络）
func fillScanStatus(st *ScanStatus, jobs map[string]int64) {
	if info, ok := conf.GetStats()[st.Network]; ok {
		st.LastBlock = info.Block
		st.LastSuccessAt = info.Time
	}
	st.SuccessRate = conf.GetSuccessRate(st.Network)
	st.LookbackActive = lookbackTrack.inflight(st.Network)
	st.AbandonedBlocks = abandonedBlocks(st.Network)
	st.CursorHeight = cursorOf(st.Network).current()
	if jobs == nil {
		jobs = map[string]int64{}
	}
	st.Jobs = jobs
}

// Overview 全局状态：各链扫描状态 + 回调 outbox + 未认单入账
type Overview struct {
	Chains                []ScanStatus `json:"chains"`
	NotifyPending         int64        `json:"notify_pending"`          // 待发送回调
	NotifyDead            int64        `json:"notify_dead"`             // 重试耗尽的回调
	NotifyOldestSeconds   int64        `json:"notify_oldest_seconds"`   // 最早待发送回调已等待秒数
	UnmatchedTransfers24h int64        `json:"unmatched_transfers_24h"` // 24 小时内打到本系统钱包但未匹配订单的入账
}

func GetOverview() Overview {
	pending, oldest, dead := model.NotifyOutboxStats()
	ov := Overview{
		Chains:                Statuses(),
		NotifyPending:         pending,
		NotifyDead:            dead,
		UnmatchedTransfers24h: model.CountUnmatchedChainTransfers(time.Now().Add(-24 * time.Hour)),
	}
	if oldest != nil {
		ov.NotifyOldestSeconds = int64(time.Since(*oldest).Seconds())
	}

	return ov
}

// Statuses 全部已启动扫描器的状态，按网络名排序
func Statuses() []ScanStatus {
	list := make([]ScanStatus, 0)
	jobs := model.ScanJobCountsAll()
	scanRegistry.Range(func(_, v any) bool {
		st := v.(scanHandle).status()
		fillScanStatus(&st, jobs[st.Network])
		list = append(list, st)

		return true
	})
	sort.Slice(list, func(i, j int) bool { return list[i].Network < list[j].Network })

	return list
}

// Replay 将 [from, to] 区间重新送入低优先级队列扫描，返回入队批次数；同时登记一条 replay 任务用于跟踪结果。
// 幂等：订单匹配只作用于待支付/确认中的订单，链上流水按 network+tx_hash+event_index 去重，
// 非订单通知按交易哈希去重，重复回放不会产生重复回调。
func Replay(network string, from, to int64) (int, error) {
	v, ok := scanRegistry.Load(network)
	if !ok {
		return 0, fmt.Errorf("网络 %s 不支持回放或尚未启动", network)
	}
	if from <= 0 || to < from {
		return 0, fmt.Errorf("区块范围无效：from=%d to=%d", from, to)
	}
	if to-from+1 > replayMaxBlocks {
		return 0, fmt.Errorf("单次回放最多 %d 个区块，当前 %d", replayMaxBlocks, to-from+1)
	}

	job, err := model.CreateScanJob(network, from, to, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		return 0, fmt.Errorf("登记回放任务失败：%w", err)
	}

	n := dispatchScanJob(v.(scanHandle), job)
	log.Task.Info(fmt.Sprintf("区块回放已入队(%s) %d → %d，批次 %d，任务 #%d", network, from, to, n, job.ID))

	return n, nil
}

// dispatchScanJob 把任务区间送入扫描器并开始跟踪进度
func dispatchScanJob(h scanHandle, job model.ScanJob) int {
	n := h.replay(job.FromHeight, job.ToHeight, job.ID)
	scanJobBegin(job, n)

	return n
}

// ---- 任务进度跟踪 ----
//
// 一个任务可能被拆成多个批次入队；全部批次成功才算完成，任一批次放弃即失败并按任务级退避重排。

type scanJobProgress struct {
	remaining atomic.Int32
	failed    atomic.Bool
	mu        sync.Mutex
	reason    string
}

var scanJobs sync.Map // jobID → *scanJobProgress

func scanJobBegin(job model.ScanJob, parts int) {
	if parts <= 0 {
		_ = job.MarkDone()

		return
	}

	p := &scanJobProgress{}
	p.remaining.Store(int32(parts))
	scanJobs.Store(job.ID, p)
}

func scanJobPartDone(jobID int64) {
	scanJobPartFinished(jobID, "")
}

func scanJobPartFailed(jobID int64, reason string) {
	scanJobPartFinished(jobID, reason)
}

func scanJobPartFinished(jobID int64, reason string) {
	v, ok := scanJobs.Load(jobID)
	if !ok {
		return
	}
	p := v.(*scanJobProgress)
	if reason != "" {
		p.failed.Store(true)
		p.mu.Lock()
		p.reason = reason
		p.mu.Unlock()
	}
	if p.remaining.Add(-1) > 0 {
		return
	}

	scanJobs.Delete(jobID)
	job, ok := model.GetScanJob(jobID)
	if !ok {
		return
	}

	if !p.failed.Load() {
		if err := job.MarkDone(); err != nil {
			log.Task.Warn(fmt.Sprintf("scan job #%d mark done failed: %v", jobID, err))
		}
		log.Task.Info(fmt.Sprintf("扫描任务完成(%s) #%d %d → %d", job.Network, job.ID, job.FromHeight, job.ToHeight))

		return
	}

	p.mu.Lock()
	reason = p.reason
	p.mu.Unlock()
	if err := job.MarkFailed(reason); err != nil {
		log.Task.Warn(fmt.Sprintf("scan job #%d mark failed error: %v", jobID, err))
	}
	if job.Status == model.ScanJobStatusFailed {
		scanAlert(fmt.Sprintf("job_dead_%d", jobID), 24*time.Hour, "扫描任务重试耗尽",
			fmt.Sprintf("网络：%s\n任务：#%d 区块 %d → %d\n已自动重试 %d 次仍失败，已停止自动重试。\n最后错误：%s\n请更换 RPC 节点后通过 POST /api/scan/replay 或 bepusdt scan replay 手动补扫。",
				job.Network, job.ID, job.FromHeight, job.ToHeight, job.Attempts, reason))
	}
}

// scanJobRetry 每分钟把到期的 pending 任务重新送入对应链的低优先级队列
func scanJobRetry(context.Context) {
	for _, job := range model.DueScanJobs(20) {
		v, ok := scanRegistry.Load(job.Network)
		if !ok { // 该链未启动，留在表里等待
			continue
		}
		h := v.(scanHandle)
		if h.status().LookbackQueue >= blockQueueLimit { // 队列拥堵，下个周期再说
			continue
		}
		if err := job.MarkRunning(); err != nil {
			log.Task.Warn(fmt.Sprintf("scan job #%d mark running failed: %v", job.ID, err))

			continue
		}

		n := dispatchScanJob(h, job)
		log.Task.Info(fmt.Sprintf("扫描任务重试(%s) #%d %d → %d 第 %d 次，批次 %d", job.Network, job.ID, job.FromHeight, job.ToHeight, job.Attempts+1, n))
	}
}

// ---- 连续扫描游标 ----
//
// 只记录"已连续完成"的最高高度：实时发出的区块登记为 pending，完成后逐个销账，
// 游标只在 height+1 已完成时前进，因此 N 失败时即使 N+1、N+2 已成功也不会越过 N。
// 放弃的区块先落库为任务再调用 complete 允许越过（见 scanAbandon）。
// 回溯 / 回放的区块不在 pending 中，complete 会忽略它们，不影响游标。

const cursorFlushInterval = 2 * time.Second

type scanCursor struct {
	mu      sync.Mutex
	network string
	height  int64
	pending map[int64]bool // 已发出的高度 → 是否已完成
	dirty   bool
	loaded  bool
}

var cursors sync.Map // network → *scanCursor

func cursorOf(network string) *scanCursor {
	v, _ := cursors.LoadOrStore(network, &scanCursor{network: network, pending: make(map[int64]bool)})

	return v.(*scanCursor)
}

// load 首次调用时从数据库读取持久化高度
func (c *scanCursor) load() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.loaded {
		c.loaded = true
		if h, ok := model.GetScanCursor(c.network); ok {
			c.height = h
		}
	}

	return c.height
}

func (c *scanCursor) current() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.height
}

// issue 实时区块发出扫描
func (c *scanCursor) issue(from, to int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for h := from; h <= to; h++ {
		if h <= c.height {
			continue
		}
		if _, ok := c.pending[h]; !ok {
			c.pending[h] = false
		}
	}
}

// complete 区块完成（成功，或已落库为失败任务），连续时推进游标
func (c *scanCursor) complete(from, to int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for h := from; h <= to; h++ {
		if _, ok := c.pending[h]; ok {
			c.pending[h] = true
		}
	}
	for {
		done, ok := c.pending[c.height+1]
		if !ok || !done {
			break
		}
		delete(c.pending, c.height+1)
		c.height++
		c.dirty = true
	}
}

// reset 链头对齐：丢弃 pending，游标直接置为指定高度（调用方负责把跳过的区间落库）
func (c *scanCursor) reset(height int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.loaded = true
	c.height = height
	c.pending = make(map[int64]bool)
	c.dirty = true
}

// pendingCount 在途未完成的实时区块数
func (c *scanCursor) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.pending)
}

func (c *scanCursor) flush() {
	c.mu.Lock()
	if !c.dirty {
		c.mu.Unlock()

		return
	}
	height := c.height
	c.dirty = false
	c.mu.Unlock()

	if err := model.SaveScanCursor(c.network, height); err != nil {
		log.Task.Warn(fmt.Sprintf("save scan cursor %s=%d failed: %v", c.network, height, err))
		c.mu.Lock()
		c.dirty = true
		c.mu.Unlock()
	}
}

func flushCursors(context.Context) {
	cursors.Range(func(_, v any) bool {
		v.(*scanCursor).flush()

		return true
	})
}

// Shutdown 进程退出前把游标落盘
func Shutdown() {
	flushCursors(context.Background())
}

// resumeFrom 计算头部同步的起点（返回"最后已发出"的高度，调用方从其 +1 开始入队）：
//   - 进程刚启动（last=0）且有持久化游标：游标与链头差距 ≤ tolerance → 从游标续扫；
//     否则把 [游标+1, 链头-1] 记为 gap 任务（不自动重试，待支付订单由回溯覆盖），从链头开始；
//   - 运行中链头跳跃超过 tolerance（长时间拥堵或节点异常）：同样记 gap 任务并对齐链头；
//   - 其它情况保持 last 不变。
//
// 任何被跳过的区间都会落库并告警，不再静默丢块。
func resumeFrom(network string, last, now, tolerance int64) int64 {
	if tolerance <= 0 {
		tolerance = 1000
	}
	c := cursorOf(network)

	if last == 0 {
		saved := c.load()
		if saved > 0 && now-saved <= tolerance {
			c.reset(saved)
			log.Task.Info(fmt.Sprintf("扫描游标恢复(%s)：从 %d 续扫至链头 %d", network, saved, now))

			return saved
		}
		if saved > 0 && now-1 > saved {
			recordGap(network, saved+1, now-1, "重启后链头已超出 block_height_max_diff，从链头继续")
		}
		c.reset(now - 1)

		return now - 1
	}

	if now-last > tolerance {
		if now-1 > last {
			recordGap(network, last+1, now-1, "链头跳跃超出 block_height_max_diff，对齐链头")
		}
		c.reset(now - 1)

		return now - 1
	}

	return last
}

// recordGap 未扫描区间落库为 deferred 任务并告警
func recordGap(network string, from, to int64, reason string) {
	if _, err := model.CreateScanJob(network, from, to, model.ScanJobKindGap, model.ScanJobStatusDeferred, reason, time.Now()); err != nil {
		log.Task.Error(fmt.Sprintf("persist scan gap %s %d-%d failed: %v", network, from, to, err))
	}
	log.Task.Warn(fmt.Sprintf("扫描区间跳过(%s) %d → %d：%s", network, from, to, reason))
	scanAlert("gap_"+network, 10*time.Minute, "扫描区间跳过",
		fmt.Sprintf("网络：%s\n区间：%d → %d（%d 个区块）\n原因：%s\n该区间已记录为 gap 任务，不会自动扫描；期间的待支付订单会由订单回溯覆盖。\n如需完整补扫可用 POST /api/scan/replay 分段回放（单次 ≤ %d 个区块）。",
			network, from, to, to-from+1, reason, replayMaxBlocks))
}

// blockHeightTolerance block_height_max_diff 配置
func blockHeightTolerance() int64 {
	n := cast.ToInt64(model.GetC(model.BlockHeightMaxDiff))
	if n <= 0 {
		return 1000
	}

	return n
}

// ---- 队列：实时优先 ----

// takeJob 优先消费实时队列，实时队列为空时才消费回溯队列；ctx 结束返回 false
func takeJob[T any](ctx context.Context, realtime, lookback *chanx.UnboundedChan[T]) (T, bool) {
	var zero T

	select {
	case j := <-realtime.Out:
		return j, true
	default:
	}

	select {
	case j := <-realtime.Out:
		return j, true
	case j := <-lookback.Out:
		return j, true
	case <-ctx.Done():
		return zero, false
	}
}

// waitQueueRoom 回溯入队前等待队列低于上限，避免一次性把队列打满；ctx 结束返回 false
func waitQueueRoom[T any](ctx context.Context, q *chanx.UnboundedChan[T]) bool {
	for q.Len() >= blockQueueLimit {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(500 * time.Millisecond):
		}
	}

	return true
}

// scanRequired 该网络当前是否需要扫块：有 MQTT 订阅、开启了钱包监控、或存在待支付/回溯窗口内的订单。
// 结果按网络集合缓存 3 秒：十几条链每几秒各自查一遍库会产生大量重复查询，这里两条 SQL 覆盖全部网络。
func scanRequired(network string) bool {
	if mqttSubscribed(network) {
		return true
	}

	return scanRequiredSet()[network]
}

const scanRequiredCacheKey = "scan_required_networks"

func scanRequiredSet() map[string]bool {
	if v, ok := cache.Get(scanRequiredCacheKey); ok {
		return v.(map[string]bool)
	}

	set := make(map[string]bool)

	var monitored []model.TradeType
	model.Db.Model(&model.Wallet{}).Where("other_notify = ?", model.WaOtherEnable).Distinct("trade_type").Pluck("trade_type", &monitored)
	for _, t := range monitored {
		set[string(model.TradeNetwork(t))] = true
	}

	var receivable []model.TradeType
	model.Db.Model(&model.Order{}).
		Where("status in (?)", receivableOrderStatuses()).
		Where("expired_at > ?", time.Now().Add(model.GetLookbackHour())).
		Distinct("trade_type").Pluck("trade_type", &receivable)
	for _, t := range receivable {
		set[string(model.TradeNetwork(t))] = true
	}

	cache.Set(scanRequiredCacheKey, set, 3*time.Second)

	return set
}

// syncBreak 实时同步是否应暂停：实时队列拥堵，或当前没有任何扫块需求
func syncBreak(network string, num int) bool {
	if num >= blockQueueLimit {
		log.Task.Warn(fmt.Sprintf("%s 同步阻塞，当前区块消费堆积数量：%d", network, num))
		scanAlert("queue_"+network, 10*time.Minute, "扫描队列拥堵",
			fmt.Sprintf("网络：%s\n实时队列堆积 %d 个区块已达上限 %d，新区块暂停入队。\n通常是 RPC 节点限流或不可用，请检查节点配置。", network, num, blockQueueLimit))

		return true
	}

	return !scanRequired(network)
}

// ---- 健康巡检 ----

const scanStallAlertAfter = 2 * time.Minute // 有扫块需求但超过该时长没有任何成功扫描即告警

var scanStartedAt = time.Now()

func init() {
	Register(Task{Duration: time.Minute, Callback: scanHealthCheck})
	Register(Task{Duration: time.Minute, Callback: scanJobRetry})
	Register(Task{Duration: cursorFlushInterval, Callback: flushCursors})
}

const notifyBacklogAlertAfter = 30 * time.Minute // 待发送回调等待超过该时长即告警

// scanHealthCheck 每分钟输出各链扫描健康状况；有待处理订单而扫描停滞、回调积压时告警
func scanHealthCheck(context.Context) {
	ov := GetOverview()
	log.Task.WithFields(logrus.Fields{
		"notify_pending":          ov.NotifyPending,
		"notify_dead":             ov.NotifyDead,
		"notify_oldest_seconds":   ov.NotifyOldestSeconds,
		"unmatched_transfers_24h": ov.UnmatchedTransfers24h,
	}).Info("notify health")

	if ov.NotifyPending > 0 && time.Duration(ov.NotifyOldestSeconds)*time.Second > notifyBacklogAlertAfter {
		scanAlert("notify_backlog", 10*time.Minute, "商户回调积压",
			fmt.Sprintf("有 %d 个订单回调尚未被商户确认，最早一条已等待 %s。\n请检查商户系统（回调地址）是否可用；回调会按退避持续重试，重试耗尽的订单可在后台手动重发。",
				ov.NotifyPending, (time.Duration(ov.NotifyOldestSeconds)*time.Second).Round(time.Minute)))
	}

	for _, st := range ov.Chains {
		if !scanRequired(st.Network) {
			continue
		}

		stalled := time.Since(scanStartedAt)
		if st.LastSuccessAt > 0 {
			stalled = time.Since(time.Unix(st.LastSuccessAt, 0))
		}

		entry := log.Task.WithFields(logrus.Fields{
			"network":          st.Network,
			"provider":         providerHost(st.Endpoint),
			"head_height":      st.HeadHeight,
			"last_block":       st.LastBlock,
			"scan_lag":         stalled.Round(time.Second).String(),
			"success_rate":     st.SuccessRate,
			"realtime_queue":   st.RealtimeQueue,
			"lookback_queue":   st.LookbackQueue,
			"lookback_active":  st.LookbackActive,
			"abandoned_blocks": st.AbandonedBlocks,
			"cursor_height":    st.CursorHeight,
			"jobs_pending":     st.Jobs[model.ScanJobStatusPending],
			"jobs_failed":      st.Jobs[model.ScanJobStatusFailed],
		})
		entry.Info("scan health")

		if stalled > scanStallAlertAfter {
			scanAlert("stall_"+st.Network, 10*time.Minute, "区块扫描停滞",
				fmt.Sprintf("网络：%s\n当前节点：%s\n已有 %s 没有任何成功扫描，但存在待支付订单或监控需求。\n最近成功区块：%s，成功率：%s，实时队列：%d，回溯队列：%d。\n请检查 RPC 节点是否可用。",
					st.Network, providerHost(st.Endpoint), stalled.Round(time.Second), st.LastBlock, st.SuccessRate, st.RealtimeQueue, st.LookbackQueue))
		}
	}
}
