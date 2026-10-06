package migration

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func nativeMigrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "ledger.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB, _ := db.DB(); _ = sqlDB.Close() })
	if err := db.Table("bep_chain_transfer").AutoMigrate(&nativeLedgerRow{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE UNIQUE INDEX ledger_event ON bep_chain_transfer(network, tx_hash, event_index)").Error; err != nil {
		t.Fatal(err)
	}
	return db
}

func TestNativeEventIndexMigrationPreservesMatchedPaymentAndERC20(t *testing.T) {
	db := nativeMigrationDB(t)
	legacy := nativeLedgerRow{Network: "ethereum", TxHash: "0xpayment", TradeType: "ethereum.eth", EventIndex: 71,
		MatchStatus: "matched", OrderID: 123, BlockNum: 200, FromAddress: "from", ToAddress: "to", Amount: "1", BlockTime: time.Unix(1700000000, 0)}
	token := legacy
	token.TradeType, token.EventIndex, token.OrderID = "usdt.erc20", 0, 124
	if err := db.Table("bep_chain_transfer").Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Table("bep_chain_transfer").Create(&token).Error; err != nil {
		t.Fatal(err)
	}
	if err := m202610061800EVMNativeEventIndex().Migrate(db); err != nil {
		t.Fatal(err)
	}
	var got nativeLedgerRow
	db.Table("bep_chain_transfer").Where("id = ?", legacy.ID).First(&got)
	if got.EventIndex != -1 || got.MatchStatus != "matched" || got.OrderID != 123 {
		t.Fatalf("native migration must retain consumed payment identity, got %+v", got)
	}
	// 新扫描器再插入同一 native(-1) 时只能命中旧记录，不产生可用于第二单的 unmatched 流水。
	replayed := legacy
	replayed.ID, replayed.EventIndex, replayed.OrderID, replayed.MatchStatus = 0, -1, 0, "unmatched"
	if err := db.Table("bep_chain_transfer").Clauses(clause.OnConflict{DoNothing: true}).Create(&replayed).Error; err != nil {
		t.Fatal(err)
	}
	var rows []nativeLedgerRow
	db.Table("bep_chain_transfer").Order("id asc").Find(&rows)
	if len(rows) != 2 || rows[1].EventIndex != 0 || rows[1].OrderID != 124 {
		t.Fatalf("replay must not add a native payment or modify ERC20 log zero, got %+v", rows)
	}
	if err := m202610061800EVMNativeEventIndex().Migrate(db); err != nil {
		t.Fatal("migration must be idempotent:", err)
	}
}

func TestNativeEventIndexMigrationMergesOnlyIdenticalDuplicates(t *testing.T) {
	db := nativeMigrationDB(t)
	matched := nativeLedgerRow{Network: "bsc", TxHash: "bnb", TradeType: "bsc.bnb", EventIndex: 11,
		MatchStatus: "matched", OrderID: 99, BlockNum: 100, Amount: "2", BlockTime: time.Unix(1700000000, 0)}
	duplicate := matched
	duplicate.EventIndex, duplicate.MatchStatus, duplicate.OrderID = -1, "unmatched", 0
	for _, row := range []*nativeLedgerRow{&matched, &duplicate} {
		if err := db.Table("bep_chain_transfer").Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m202610061800EVMNativeEventIndex().Migrate(db); err != nil {
		t.Fatal(err)
	}
	var rows []nativeLedgerRow
	db.Table("bep_chain_transfer").Find(&rows)
	if len(rows) != 1 || rows[0].ID != matched.ID || rows[0].OrderID != 99 || rows[0].EventIndex != -1 {
		t.Fatalf("the consumed original must remain canonical, got %+v", rows)
	}
}

func TestNativeEventIndexMigrationRejectsConflictingOrderClaims(t *testing.T) {
	db := nativeMigrationDB(t)
	for _, idx := range []int{0, 10} {
		row := nativeLedgerRow{Network: "ethereum", TxHash: "conflict", TradeType: "ethereum.eth", EventIndex: idx,
			MatchStatus: "matched", OrderID: int64(idx + 1), BlockTime: time.Unix(1700000000, 0)}
		if err := db.Table("bep_chain_transfer").Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m202610061800EVMNativeEventIndex().Migrate(db); err == nil {
		t.Fatal("conflicting claimed orders must stop migration")
	}
	var rows []nativeLedgerRow
	db.Table("bep_chain_transfer").Order("id asc").Find(&rows)
	if len(rows) != 2 || rows[0].EventIndex != 0 || rows[1].EventIndex != 10 {
		t.Fatalf("failed migration must preserve all original evidence, got %+v", rows)
	}
}
