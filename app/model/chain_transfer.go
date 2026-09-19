package model

import (
	"time"

	"gorm.io/gorm/clause"
)

// 链上转账流水：只记录打到本系统钱包地址的入账。
// 扫描到转账后先落库再匹配订单，订单匹配临时失败时可由对账任务重新认单；
// 唯一键 network + tx_hash + event_index，同一交易内多笔转账互不覆盖，重复扫描不会产生重复记录。

const (
	ChainTransferUnmatched = "unmatched" // 尚未匹配到订单
	ChainTransferMatched   = "matched"   // 已匹配订单
)

type ChainTransfer struct {
	Id
	Network     string    `gorm:"column:network;type:varchar(20);not null;uniqueIndex:idx_chain_transfer_event,priority:1;comment:网络" json:"network"`
	TxHash      string    `gorm:"column:tx_hash;type:varchar(128);not null;uniqueIndex:idx_chain_transfer_event,priority:2;comment:交易哈希" json:"tx_hash"`
	EventIndex  int       `gorm:"column:event_index;not null;default:0;uniqueIndex:idx_chain_transfer_event,priority:3;comment:交易内事件序号" json:"event_index"`
	BlockNum    int64     `gorm:"column:block_num;not null;default:0;comment:区块/slot/version" json:"block_num"`
	FromAddress string    `gorm:"column:from_address;type:varchar(128);not null;default:'';comment:付款地址" json:"from_address"`
	ToAddress   string    `gorm:"column:to_address;type:varchar(128);not null;index;comment:收款地址" json:"to_address"`
	TradeType   TradeType `gorm:"column:trade_type;type:varchar(20);not null;comment:交易类型" json:"trade_type"`
	Amount      string    `gorm:"column:amount;type:varchar(64);not null;comment:数额" json:"amount"`
	BlockTime   time.Time `gorm:"column:block_time;not null;index;comment:链上时间" json:"block_time"`
	MatchStatus string    `gorm:"column:match_status;type:varchar(16);not null;default:'unmatched';index;comment:unmatched/matched" json:"match_status"`
	OrderID     int64     `gorm:"column:order_id;not null;default:0;comment:匹配的订单ID" json:"order_id"`
	AutoTimeAt
}

func (ChainTransfer) TableName() string {
	return "bep_chain_transfer"
}

// SaveChainTransfers 幂等写入：唯一键冲突直接忽略
func SaveChainTransfers(list []ChainTransfer) error {
	if len(list) == 0 {
		return nil
	}

	return Db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(list, 100).Error
}

func MarkChainTransferMatched(network, txHash string, eventIndex int, orderID int64) error {
	return Db.Model(&ChainTransfer{}).
		Where("network = ? and tx_hash = ? and event_index = ?", network, txHash, eventIndex).
		Updates(map[string]any{"match_status": ChainTransferMatched, "order_id": orderID}).Error
}

// UnmatchedChainTransfers 指定时间之后仍未匹配到订单的入账
func UnmatchedChainTransfers(since time.Time, limit int) []ChainTransfer {
	rows := make([]ChainTransfer, 0)
	Db.Where("match_status = ? and block_time >= ?", ChainTransferUnmatched, since).
		Order("id asc").Limit(limit).Find(&rows)

	return rows
}

func CountUnmatchedChainTransfers(since time.Time) int64 {
	var count int64
	Db.Model(&ChainTransfer{}).Where("match_status = ? and block_time >= ?", ChainTransferUnmatched, since).Count(&count)

	return count
}
