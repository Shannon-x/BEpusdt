package migration

import (
	"fmt"
	"time"

	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

// 原生币不是 ERC20 日志：用 -1 与非负 logIndex 隔离。
// 升级时必须迁移旧 transactionIndex，否则重放同一 ETH/BNB 付款会生成第二条可认单流水。
func m202610061800EVMNativeEventIndex() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202610061800_evm_native_event_index",
		Migrate: func(db *gorm.DB) error {
			if !db.Migrator().HasTable("bep_chain_transfer") {
				return nil
			}
			return db.Transaction(func(tx *gorm.DB) error {
				var afterID int64
				for {
					var page []nativeLedgerRow
					if err := tx.Table("bep_chain_transfer").Where("id > ? and event_index >= 0 and ((network = ? and trade_type = ?) or (network = ? and trade_type = ?))",
						afterID, "ethereum", "ethereum.eth", "bsc", "bsc.bnb").Order("id asc").Limit(200).Find(&page).Error; err != nil {
						return err
					}
					if len(page) == 0 {
						return nil
					}
					afterID = page[len(page)-1].ID
					for _, row := range page {
						var same []nativeLedgerRow
						if err := tx.Table("bep_chain_transfer").Where("network = ? and tx_hash = ? and trade_type = ?", row.Network, row.TxHash, row.TradeType).Order("id asc").Find(&same).Error; err != nil {
							return err
						}
						if len(same) == 0 {
							continue // 同一分页内的重复项可能已经合并
						}
						keep := same[0]
						for _, candidate := range same {
							if candidate.MatchStatus == "matched" {
								keep = candidate
								break
							}
						}
						var duplicates []int64
						for _, candidate := range same {
							if !keep.samePayment(candidate) || (candidate.MatchStatus == "matched" && candidate.OrderID != keep.OrderID) {
								return fmt.Errorf("native ledger #%d has conflicting duplicate #%d; reconcile before upgrading", keep.ID, candidate.ID)
							}
							if candidate.ID != keep.ID {
								duplicates = append(duplicates, candidate.ID)
							}
						}
						if len(duplicates) > 0 {
							if err := tx.Table("bep_chain_transfer").Where("id in ?", duplicates).Delete(&nativeLedgerRow{}).Error; err != nil {
								return err
							}
						}
						if err := tx.Table("bep_chain_transfer").Where("id = ?", keep.ID).Update("event_index", -1).Error; err != nil {
							return err
						}
					}
				}
			})
		},
		// 已有新格式流水后不自动逆转唯一键，避免回滚时覆盖 ERC20 事件。
		Rollback: func(*gorm.DB) error { return nil },
	}
}

type nativeLedgerRow struct {
	ID                             int64
	Network, TxHash, TradeType     string
	EventIndex                     int
	MatchStatus                    string
	OrderID, BlockNum              int64
	FromAddress, ToAddress, Amount string
	BlockTime                      time.Time
}

func (r nativeLedgerRow) samePayment(other nativeLedgerRow) bool {
	return r.Network == other.Network && r.TxHash == other.TxHash && r.TradeType == other.TradeType &&
		r.BlockNum == other.BlockNum && r.BlockTime.Equal(other.BlockTime) &&
		r.FromAddress == other.FromAddress && r.ToAddress == other.ToAddress && r.Amount == other.Amount
}
