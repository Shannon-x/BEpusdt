package task

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
)

func TestSolanaPersistsBeforeCompletingAndRetriesDatabaseFailure(t *testing.T) {
	const slot = 500000123
	const hash = "solana-database-retry-payment"
	ensureWallet(t, model.UsdtSolana, testSolRecvOwner)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.ReplaceAll(solanaTestBlock(slot), testSolSignature, hash)))
	}))
	defer srv.Close()
	s := newTestSolana(srv.URL)
	c := cursorOf(conf.Solana)
	c.reset(slot - 1)
	c.issue(slot, slot)
	if err := model.Db.Exec(fmt.Sprintf(`CREATE TRIGGER reject_sol_payment BEFORE INSERT ON bep_chain_transfer WHEN NEW.tx_hash = '%s' BEGIN SELECT RAISE(ABORT, 'storage unavailable'); END`, hash)).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Exec("DROP TRIGGER IF EXISTS reject_sol_payment") })

	s.scanSlot(solanaSlot{Slot: slot})
	if c.current() != slot-1 {
		t.Fatal("database failure must not advance the scan cursor")
	}
	if _, ok := recvTransfers(t, 20*time.Millisecond); ok {
		t.Fatal("a transfer must not be queued for matching before persistence succeeds")
	}
	var retry solanaSlot
	select {
	case retry = <-s.slotQueue.Out:
	case <-time.After(time.Second):
		t.Fatal("database failure must schedule a retry")
	}
	if err := model.Db.Exec("DROP TRIGGER reject_sol_payment").Error; err != nil {
		t.Fatal(err)
	}
	s.scanSlot(retry)
	if c.current() != slot {
		t.Fatalf("successful persistence must complete the block, cursor=%d", c.current())
	}
	if transfers, ok := recvTransfers(t, time.Second); !ok || len(transfers) != 1 || transfers[0].TxHash != hash {
		t.Fatalf("recovered payment must be queued, got %+v", transfers)
	}
	var count int64
	if err := model.Db.Model(&model.ChainTransfer{}).Where("tx_hash = ?", hash).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("payment must be durably recorded exactly once: count=%d error=%v", count, err)
	}
}

func TestTronOverlappingReplaysCompleteBothJobs(t *testing.T) {
	s := newTron()
	makeJob := func() model.ScanJob {
		job, err := model.CreateScanJob(conf.Tron, 900, 900, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		dispatchScanJob(scanHandle{replay: s.replay}, job)
		return job
	}
	first, second := makeJob(), makeJob()
	s.blockDone(900)
	for _, job := range []model.ScanJob{first, second} {
		got, ok := model.GetScanJob(job.ID)
		if !ok || got.Status != model.ScanJobStatusDone {
			t.Fatalf("overlapping task #%d must be completed, got %+v", job.ID, got)
		}
	}
}

func TestLegacyTronAndAptosIndicesRemainStableAcrossUpgradeAndMergedBatches(t *testing.T) {
	for _, tc := range []struct {
		network string
		trade   model.TradeType
	}{{conf.Tron, model.UsdtTrc20}, {conf.Aptos, model.UsdtAptos}} {
		t.Run(tc.network, func(t *testing.T) {
			addr, hash := "legacy-wallet-"+tc.network, "legacy-payment-"+tc.network
			ensureWallet(t, tc.trade, addr)
			now := time.Now().Truncate(time.Second)
			old := model.ChainTransfer{Network: tc.network, TxHash: hash, EventIndex: 0,
				FromAddress: "payer", ToAddress: addr, TradeType: tc.trade, Amount: "5", BlockNum: 100,
				BlockTime: now.Add(-time.Minute), MatchStatus: model.ChainTransferMatched, OrderID: 999}
			if err := model.SaveChainTransfers([]model.ChainTransfer{old}); err != nil {
				t.Fatal(err)
			}
			order := newTestOrder(t, tc.trade, addr, "5", model.OrderStatusWaiting, now.Add(-time.Hour), now.Add(time.Hour), "")
			batch := []transfer{
				{Network: tc.network, TxHash: hash, Amount: decimal.NewFromInt(9), FromAddress: "payer", RecvAddress: "outside-wallet", TradeType: tc.trade, BlockNum: 100, Timestamp: old.BlockTime},
				{Network: tc.network, TxHash: hash, Amount: decimal.NewFromInt(5), FromAddress: "payer", RecvAddress: addr, TradeType: tc.trade, BlockNum: 100, Timestamp: old.BlockTime},
				{Network: tc.network, TxHash: hash, Amount: decimal.NewFromInt(6), FromAddress: "payer", RecvAddress: addr, TradeType: tc.trade, BlockNum: 100, Timestamp: old.BlockTime},
			}
			if err := persistTransfers(batch); err != nil {
				t.Fatal(err)
			}
			if batch[1].Index != 0 || batch[2].Index != 1 || !batch[1].IndexAssigned {
				t.Fatalf("indices must retain legacy numbering after wallet filtering, got %+v", batch)
			}
			merged := append(append([]transfer{}, batch...), batch...)
			if _, err := processTransferBatch(merged); err != nil {
				t.Fatal(err)
			}
			var rows []model.ChainTransfer
			model.Db.Where("network = ? and tx_hash = ?", tc.network, hash).Order("event_index asc").Find(&rows)
			if len(rows) != 2 || rows[0].MatchStatus != model.ChainTransferMatched || rows[0].OrderID != 999 {
				t.Fatalf("replay/merged batches must not create extra payments, got %+v", rows)
			}
			got, _ := model.GetOrderByID(order.ID)
			if got.Status != model.OrderStatusWaiting {
				t.Fatal("legacy consumed payment must not match a second order")
			}
		})
	}
}
