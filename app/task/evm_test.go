package task

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
)

const (
	testPolFrom   = 93363010
	testPolTo     = 93363019
	testPolTxHash = "0xa0fa5d0a6a1f4c8b9e2d3c4b5a6978877665544332211009988776655443fb47"
	testPolRecv   = "0x1111111111111111111111111111111111111111"
	testPolSender = "0x2222222222222222222222222222222222222222"
)

type evmMock struct {
	missingBlock int64  // 批量响应中省略该区块
	errorBlock   int64  // 批量响应中该区块返回 error
	nullBlock    int64  // 批量响应中该区块 result 为 null（节点落后）
	blockStatus  int    // eth_getBlockByNumber 的 HTTP 状态，0 表示 200
	logsResp     string // 每个 eth_getLogs 子响应的 result/error 原文（JSON 片段），空表示正常返回
	logsRequests []string
}

func testBlockHash(n int64) string {
	return fmt.Sprintf("0x%064x", n*7919)
}

func (m *evmMock) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s := string(body)
	batch := gjson.Parse(s).Array()
	if len(batch) == 0 {
		w.WriteHeader(400)

		return
	}

	switch batch[0].Get("method").String() {
	case "eth_getBlockByNumber":
		if m.blockStatus != 0 {
			w.WriteHeader(m.blockStatus)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))

			return
		}

		items := make([]string, 0)
		for _, req := range batch {
			id := req.Get("id").Int()
			if id == m.missingBlock {
				continue
			}
			if id == m.errorBlock {
				items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"block not found"}}`, id))

				continue
			}
			if id == m.nullBlock {
				items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":null}`, id))

				continue
			}
			items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x%x","hash":"%s","timestamp":"0x%x","transactions":[]}}`, id, id, testBlockHash(id), 1700000000+id))
		}
		_, _ = w.Write([]byte("[" + strings.Join(items, ",") + "]"))

	case "eth_getLogs":
		m.logsRequests = append(m.logsRequests, s)
		items := make([]string, 0)
		for _, req := range batch {
			id := req.Get("id").Int()
			if m.logsResp != "" {
				items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,%s}`, id, m.logsResp))

				continue
			}
			if id != testPolFrom+3 { // 只有这个块有目标转账
				items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":[]}`, id))

				continue
			}
			log := map[string]any{
				"address":         conf.UsdtPolygon,
				"topics":          []string{evmTransferEvent, "0x000000000000000000000000" + testPolSender[2:], "0x000000000000000000000000" + testPolRecv[2:]},
				"data":            "0x0000000000000000000000000000000000000000000000000000000000445c60", // 4480096 → 4.480096 USDT
				"blockNumber":     fmt.Sprintf("0x%x", id),
				"blockHash":       req.Get("params.0.blockHash").String(),
				"transactionHash": testPolTxHash,
				"logIndex":        "0x4",
			}
			logJSON, _ := json.Marshal(log)
			items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":[%s]}`, id, logJSON))
		}
		_, _ = w.Write([]byte("[" + strings.Join(items, ",") + "]"))

	default:
		w.WriteHeader(404)
	}
}

func newTestEvm(t *testing.T, m *evmMock) *evm {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(m.handler))
	t.Cleanup(srv.Close)

	e := newEvm(conf.Polygon, block{}, evmNative{}, 0)
	e.rpc.override = []string{srv.URL}

	return e
}

func recvBlockRetry(t *testing.T, e *evm, wait time.Duration) (evmBlock, bool) {
	t.Helper()
	select {
	case job := <-e.blockScanQueue.Out:
		return job, true
	case <-time.After(wait):
		return evmBlock{}, false
	}
}

func TestEvmGetLogsFiltersByNetworkContractsAndParsesTransfer(t *testing.T) {
	m := &evmMock{}
	e := newTestEvm(t, m)

	e.scanBlocks(evmBlock{From: testPolFrom, To: testPolTo})

	if len(m.logsRequests) != 1 {
		t.Fatalf("expected one batched eth_getLogs request, got %d", len(m.logsRequests))
	}
	reqs := gjson.Parse(m.logsRequests[0]).Array()
	if len(reqs) != testPolTo-testPolFrom+1 {
		t.Fatalf("logs must be requested per block in one batch, got %d items", len(reqs))
	}
	for _, req := range reqs {
		if req.Get("params.0.blockHash").String() != testBlockHash(req.Get("id").Int()) {
			t.Fatalf("each eth_getLogs must be scoped to the validated block hash, got %s", req.Get("params.0").Raw)
		}
		if req.Get("params.0.fromBlock").Exists() || req.Get("params.0.toBlock").Exists() {
			t.Fatal("range filters must not be combined with blockHash")
		}
		addrs := req.Get("params.0.address").Array()
		if len(addrs) != 2 {
			t.Fatalf("eth_getLogs must filter by the network's enabled contracts, got %s", req.Get("params.0.address").Raw)
		}
		seen := map[string]bool{}
		for _, a := range addrs {
			seen[a.String()] = true
		}
		if !seen[conf.UsdtPolygon] || !seen[conf.UsdcPolygon] {
			t.Fatalf("address filter must contain polygon USDT and USDC, got %v", seen)
		}
		if req.Get("params.0.topics.0").String() != evmTransferEvent {
			t.Fatal("topics must contain the Transfer event signature")
		}
	}

	transfers, ok := recvTransfers(t, time.Second)
	if !ok || len(transfers) != 1 {
		t.Fatalf("expected one transfer, got %v", transfers)
	}
	tr := transfers[0]
	if tr.TxHash != testPolTxHash || tr.TradeType != model.UsdtPolygon || tr.RecvAddress != testPolRecv || tr.FromAddress != testPolSender || tr.Amount.String() != "4.480096" || tr.BlockNum != testPolFrom+3 || tr.Index != 4 {
		t.Fatalf("unexpected transfer: %+v", tr)
	}
	if tr.Timestamp.Unix() != 1700000000+testPolFrom+3 {
		t.Fatalf("transfer timestamp must come from its own block, got %d", tr.Timestamp.Unix())
	}
	if got := conf.GetStats()[conf.Polygon].Block; got != fmt.Sprint(testPolTo) {
		t.Fatalf("success must be recorded after logs parsed, got %q", got)
	}
	if job, ok := recvBlockRetry(t, e, 100*time.Millisecond); ok {
		t.Fatalf("successful batch must not be re-enqueued: %+v", job)
	}
}

func TestEvmBatchFailuresAreNotSuccessAndRetry(t *testing.T) {
	cases := []struct {
		name string
		from int64
		mock *evmMock
	}{
		{"batch missing one block", 93363020, &evmMock{missingBlock: 93363022}},
		{"batch block error", 93363030, &evmMock{errorBlock: 93363031}},
		{"http 429", 93363040, &evmMock{blockStatus: 429}},
		{"http 500", 93363050, &evmMock{blockStatus: 500}},
		{"eth_getLogs rpc error", 93363060, &evmMock{logsResp: `"error":{"code":-32000,"message":"unknown block"}`}},
		{"eth_getLogs result not array", 93363070, &evmMock{logsResp: `"result":null`}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEvm(t, tc.mock)
			to := tc.from + 2

			e.scanBlocks(evmBlock{From: tc.from, To: to})

			if got := conf.GetStats()[conf.Polygon].Block; got == fmt.Sprint(to) {
				t.Fatal("failed batch must not be recorded as success")
			}
			job, ok := recvBlockRetry(t, e, 2*time.Second)
			if !ok {
				t.Fatal("failed batch must be re-enqueued for retry")
			}
			if job.From != tc.from || job.To != to || job.Attempt != 1 {
				t.Fatalf("unexpected retry job: %+v", job)
			}
			if _, ok := recvTransfers(t, 50*time.Millisecond); ok {
				t.Fatal("no transfers expected on failure")
			}
		})
	}
}

func TestEvmLaggingNodeWaitsBeforeSwitchingEndpoint(t *testing.T) {
	m := &evmMock{nullBlock: 93363102}
	e := newTestEvm(t, m)
	primary := e.rpc.override[0]
	e.rpc.override = []string{primary, "http://127.0.0.1:9/backup"}

	// 第一次：区块尚未可用，原节点等待，不切换
	e.scanBlocks(evmBlock{From: 93363100, To: 93363102})
	if job, ok := recvBlockRetry(t, e, 2*time.Second); !ok || job.Attempt != 1 {
		t.Fatalf("null block must be retried, got %+v ok=%v", job, ok)
	}
	if e.rpc.current() != primary {
		t.Fatal("first not-yet-available must not switch endpoint")
	}

	// 第三次仍不可用：切换到备用节点
	e.scanBlocks(evmBlock{From: 93363100, To: 93363102, Attempt: 2})
	if _, ok := recvBlockRetry(t, e, 2*time.Second); !ok {
		t.Fatal("still-unavailable block must be retried")
	}
	if e.rpc.current() == primary {
		t.Fatal("persistently unavailable block must switch endpoint")
	}
}

func TestEvmRetryExhaustionReleasesLookbackOrderAndAlerts(t *testing.T) {
	e := newTestEvm(t, &evmMock{blockStatus: 503})
	const from = 93363080
	lookbackTrack.begin(conf.Polygon, []int64{9101})
	lookbackTrack.track(conf.Polygon, from)
	lookbackTrack.sealed(conf.Polygon)
	alerts := recordAlerts(t)

	e.scanBlocks(evmBlock{From: from, To: from + 2, Attempt: scanRetryMaxAttempts - 1})

	if job, ok := recvBlockRetry(t, e, 100*time.Millisecond); ok {
		t.Fatalf("exhausted batch must not be re-enqueued: %+v", job)
	}
	if lookbackTrack.inflight(conf.Polygon) || lookbackTrack.skip(9101) {
		t.Fatal("order must return to pending after the batch was abandoned")
	}
	if len(*alerts) != 1 || (*alerts)[0].title != "区块扫描放弃" {
		t.Fatalf("abandoning a batch must raise one alert, got %+v", *alerts)
	}
}

func TestEvmLookbackRetryStaysOnLookbackQueue(t *testing.T) {
	e := newTestEvm(t, &evmMock{blockStatus: 429})

	e.scanBlocks(evmBlock{From: 93363090, To: 93363092, Lookback: true})

	if job, ok := recvBlockRetry(t, e, 100*time.Millisecond); ok {
		t.Fatalf("lookback retry must not be pushed onto the realtime queue: %+v", job)
	}
	select {
	case job := <-e.lookbackQueue.Out:
		if !job.Lookback || job.Attempt != 1 {
			t.Fatalf("unexpected lookback retry job: %+v", job)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lookback retry was not re-enqueued on the lookback queue")
	}
}

func TestEvmReplayEnqueuesBatchesOnLookbackQueue(t *testing.T) {
	e := newEvm(conf.Polygon, block{}, evmNative{}, 0)

	n := e.replay(testPolFrom, testPolTo, 0) // 10 个区块，默认批量 3 → 4 批
	if n != 4 {
		t.Fatalf("expected 4 batches, got %d", n)
	}
	var got []evmBlock
	for i := 0; i < n; i++ {
		select {
		case job := <-e.lookbackQueue.Out:
			got = append(got, job)
		case <-time.After(time.Second):
			t.Fatal("replay batches missing from lookback queue")
		}
	}
	if got[0].From != testPolFrom || got[0].To != testPolFrom+2 || got[3].From != testPolTo || got[3].To != testPolTo || !got[0].Lookback {
		t.Fatalf("unexpected replay batches: %+v", got)
	}
	if e.blockScanQueue.Len() != 0 {
		t.Fatal("replay must not touch the realtime queue")
	}
}

func TestEvmBatchSizeDefaultsToThree(t *testing.T) {
	e := &evm{Network: conf.Polygon}
	if got := e.batchSize(); got != defaultBlockBatchSize {
		t.Fatalf("expected default batch size %d, got %d", defaultBlockBatchSize, got)
	}
}
