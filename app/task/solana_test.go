package task

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
)

const (
	testSolSlot      = 448096702
	testSolSignature = "5KtPn1LGuxhFiwjxErkxTb3gQ9kQZ6FQsYoXjCkzeyYqjEQxhPr4Vk2cFf3Mb6X9YfJ6P5oUJzHf9rG6Y2m4pQ9x"
	testSolFromOwner = "FromOwnerWallet1111111111111111111111111111"
	testSolRecvOwner = "RecvOwnerWallet1111111111111111111111111111"
)

func newTestSolana(endpoints ...string) *solana {
	s := newSolana()
	s.rpc.override = endpoints

	return &s
}

// solanaTransferCheckedData 构造 SPL Token TransferChecked 指令数据：tag(12) + amount(u64 LE) + decimals
func solanaTransferCheckedData(amount uint64, decimals byte) string {
	data := make([]byte, 10)
	data[0] = 12
	binary.LittleEndian.PutUint64(data[1:9], amount)
	data[9] = decimals

	return base58.Encode(data)
}

// solanaTestBlock 一个包含两笔交易的区块：第一笔执行失败（meta.err 非空），第二笔为 4.478 USDT 的 TransferChecked
func solanaTestBlock(slot int) string {
	// accountKeys: 0 signer, 1 from token account, 2 mint, 3 recv token account, 4 SPL Token program
	accountKeys := []string{"SignerWallet11111111111111111111111111111111", "FromTokenAcct1111111111111111111111111111111", conf.UsdtSolana, "RecvTokenAcct1111111111111111111111111111111", conf.SolSplToken}
	instr := map[string]any{
		"programIdIndex": 4,
		"accounts":       []int{1, 2, 3, 0},
		"data":           solanaTransferCheckedData(4478000, 6),
	}
	tokenBalances := []map[string]any{
		{"accountIndex": 1, "mint": conf.UsdtSolana, "owner": testSolFromOwner, "programId": conf.SolSplToken},
		{"accountIndex": 3, "mint": conf.UsdtSolana, "owner": testSolRecvOwner, "programId": conf.SolSplToken},
	}
	makeTx := func(sig string, txErr any) map[string]any {
		return map[string]any{
			"transaction": map[string]any{
				"signatures": []string{sig},
				"message": map[string]any{
					"accountKeys":  accountKeys,
					"instructions": []any{instr},
				},
			},
			"meta": map[string]any{
				"err":               txErr,
				"postTokenBalances": tokenBalances,
				"preTokenBalances":  tokenBalances,
				"innerInstructions": []any{},
				"loadedAddresses":   map[string]any{"readonly": []string{}, "writable": []string{}},
			},
			"version": 0,
		}
	}

	block := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"result": map[string]any{
			"blockTime":    1700000000,
			"blockHeight":  slot - 100,
			"parentSlot":   slot - 1,
			"transactions": []any{makeTx("FailedTxSignature1111111111111111111111111111111111111111111111111111111111111111111111", map[string]any{"InstructionError": []any{0, "Custom"}}), makeTx(testSolSignature, nil)},
		},
	}
	b, _ := json.Marshal(block)

	return string(b)
}

func recvSlotRetry(t *testing.T, s *solana, wait time.Duration) (solanaSlot, bool) {
	t.Helper()
	select {
	case job := <-s.slotQueue.Out:
		return job, true
	case <-time.After(wait):
		return solanaSlot{}, false
	}
}

func recvTransfers(t *testing.T, wait time.Duration) ([]transfer, bool) {
	t.Helper()
	select {
	case ts := <-transferQueue.Out:
		return ts, true
	case <-time.After(wait):
		return nil, false
	}
}

func solanaRpcServer(t *testing.T, handler func(body string) (int, string)) (*httptest.Server, *[]string) {
	t.Helper()
	requests := make([]string, 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, string(body))
		status, resp := handler(string(body))
		w.WriteHeader(status)
		_, _ = w.Write([]byte(resp))
	}))
	t.Cleanup(srv.Close)

	return srv, &requests
}

func TestSolanaRequestsTxVersion1AndParsesSuccessfulTransferOnly(t *testing.T) {
	srv, requests := solanaRpcServer(t, func(string) (int, string) {
		return 200, solanaTestBlock(testSolSlot)
	})
	s := newTestSolana(srv.URL)

	s.scanSlot(solanaSlot{Slot: testSolSlot})

	if len(*requests) != 1 || !strings.Contains((*requests)[0], `"maxSupportedTransactionVersion":1`) {
		t.Fatalf("getBlock must request maxSupportedTransactionVersion=1, got: %v", *requests)
	}

	transfers, ok := recvTransfers(t, time.Second)
	if !ok {
		t.Fatal("expected transfers to be queued")
	}
	if len(transfers) != 1 {
		t.Fatalf("expected exactly 1 transfer (failed tx must be skipped), got %d", len(transfers))
	}
	tr := transfers[0]
	if tr.Amount.String() != "4.478" || tr.TradeType != model.UsdtSolana || tr.RecvAddress != testSolRecvOwner || tr.FromAddress != testSolFromOwner || tr.TxHash != testSolSignature || tr.BlockNum != testSolSlot {
		t.Fatalf("unexpected transfer: %+v", tr)
	}
	if got := conf.GetStats()[conf.Solana].Block; got != fmt.Sprint(testSolSlot) {
		t.Fatalf("success must be recorded after parsing, got last block %q", got)
	}
	if job, ok := recvSlotRetry(t, s, 100*time.Millisecond); ok {
		t.Fatalf("successful slot must not be re-enqueued: %+v", job)
	}
}

func TestSolanaRpcErrorsAreNotSuccessAndRetry(t *testing.T) {
	cases := []struct {
		name string
		slot int
		resp func() (int, string)
	}{
		{"unsupported tx version -32015", 448096710, func() (int, string) {
			return 200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32015,"message":"Transaction version (1) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": 1"}}`
		}},
		{"block not available -32004", 448096711, func() (int, string) {
			return 200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"Block not available for slot 448096711"}}`
		}},
		{"result null", 448096712, func() (int, string) {
			return 200, `{"jsonrpc":"2.0","id":1,"result":null}`
		}},
		{"http 429", 448096713, func() (int, string) {
			return 429, `{"error":"rate limited"}`
		}},
		{"http 500", 448096714, func() (int, string) {
			return 500, `internal error`
		}},
		{"invalid json", 448096715, func() (int, string) {
			return 200, `<html>bad gateway</html>`
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := solanaRpcServer(t, func(string) (int, string) { return tc.resp() })
			s := newTestSolana(srv.URL)

			s.scanSlot(solanaSlot{Slot: tc.slot})

			if got := conf.GetStats()[conf.Solana].Block; got == fmt.Sprint(tc.slot) {
				t.Fatal("failed slot must not be recorded as success")
			}
			job, ok := recvSlotRetry(t, s, 2*time.Second)
			if !ok {
				t.Fatal("failed slot must be re-enqueued for retry")
			}
			if job.Slot != tc.slot || job.Attempt != 1 {
				t.Fatalf("unexpected retry job: %+v", job)
			}
			if _, ok := recvTransfers(t, 50*time.Millisecond); ok {
				t.Fatal("no transfers expected on failure")
			}
		})
	}
}

func TestSolanaSkippedSlotIsEmptySuccess(t *testing.T) {
	const slot = 448096720
	srv, _ := solanaRpcServer(t, func(string) (int, string) {
		return 200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"Slot 448096720 was skipped, or missing due to ledger jump to recent snapshot"}}`
	})
	s := newTestSolana(srv.URL)
	lookbackTrack.begin(conf.Solana, []int64{9001})
	lookbackTrack.track(conf.Solana, slot)
	lookbackTrack.sealed(conf.Solana)

	s.scanSlot(solanaSlot{Slot: slot})

	if got := conf.GetStats()[conf.Solana].Block; got != fmt.Sprint(slot) {
		t.Fatalf("skipped slot should count as success, got %q", got)
	}
	if job, ok := recvSlotRetry(t, s, 100*time.Millisecond); ok {
		t.Fatalf("skipped slot must not be retried: %+v", job)
	}
	if v, _ := lookbackTrack.state.Load(int64(9001)); v != lookbackDone {
		t.Fatal("lookback order should be marked done after skipped slot")
	}
}

func TestSolanaRetryExhaustionReleasesLookbackOrderAndAlerts(t *testing.T) {
	const slot = 448096730
	srv, _ := solanaRpcServer(t, func(string) (int, string) { return 503, "unavailable" })
	s := newTestSolana(srv.URL)
	lookbackTrack.begin(conf.Solana, []int64{9002})
	lookbackTrack.track(conf.Solana, slot)
	lookbackTrack.sealed(conf.Solana)

	alerts := recordAlerts(t)

	s.scanSlot(solanaSlot{Slot: slot, Attempt: scanRetryMaxAttempts - 1})

	if job, ok := recvSlotRetry(t, s, 100*time.Millisecond); ok {
		t.Fatalf("exhausted slot must not be re-enqueued: %+v", job)
	}
	if lookbackTrack.inflight(conf.Solana) {
		t.Fatal("lookback job should be settled after the last block was abandoned")
	}
	if lookbackTrack.skip(9002) {
		t.Fatal("order must return to pending so the next lookback cycle retries it")
	}
	if len(*alerts) != 1 || (*alerts)[0].title != "区块扫描放弃" || !strings.Contains((*alerts)[0].text, fmt.Sprint(slot)) {
		t.Fatalf("abandoning a block must raise exactly one alert naming the slot, got %+v", *alerts)
	}
	if abandonedBlocks(conf.Solana) < 1 {
		t.Fatal("abandoned counter must be incremented")
	}
}

func TestSolanaFailsOverToBackupEndpointOnRetry(t *testing.T) {
	const slot = 448096740
	primary, primaryReqs := solanaRpcServer(t, func(string) (int, string) { return 503, "primary down" })
	backup, backupReqs := solanaRpcServer(t, func(string) (int, string) { return 200, solanaTestBlock(slot) })
	s := newTestSolana(primary.URL, backup.URL)

	s.scanSlot(solanaSlot{Slot: slot})

	retry, ok := recvSlotRetry(t, s, 2*time.Second)
	if !ok || retry.Attempt != 1 {
		t.Fatalf("first failure must schedule a retry, got %+v ok=%v", retry, ok)
	}
	if got := s.rpc.current(); got != backup.URL {
		t.Fatalf("endpoint failure must switch the cursor to the backup, current=%s", got)
	}

	s.scanSlot(retry)

	transfers, ok := recvTransfers(t, time.Second)
	if !ok || len(transfers) != 1 || transfers[0].Amount.String() != "4.478" {
		t.Fatalf("retry on backup must parse the block, got %v", transfers)
	}
	if len(*primaryReqs) != 1 || len(*backupReqs) != 1 {
		t.Fatalf("expected 1 request to primary and 1 to backup, got %d / %d", len(*primaryReqs), len(*backupReqs))
	}
	if job, ok := recvSlotRetry(t, s, 100*time.Millisecond); ok {
		t.Fatalf("successful retry must not be re-enqueued: %+v", job)
	}
}

func TestSolanaBlockNotAvailableRetriesOnSameEndpoint(t *testing.T) {
	const slot = 448096750
	primary, _ := solanaRpcServer(t, func(string) (int, string) {
		return 200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"Block not available for slot 448096750"}}`
	})
	backup, _ := solanaRpcServer(t, func(string) (int, string) { return 200, "{}" })
	s := newTestSolana(primary.URL, backup.URL)

	s.scanSlot(solanaSlot{Slot: slot})

	if _, ok := recvSlotRetry(t, s, 2*time.Second); !ok {
		t.Fatal("not-yet-available block must be retried")
	}
	if got := s.rpc.current(); got != primary.URL {
		t.Fatalf("-32004 is a timing issue, not a node failure; cursor must stay on primary, got %s", got)
	}
}
