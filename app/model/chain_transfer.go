package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
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

// SaveChainTransfers 幂等写入：重复事件只刷新最后补录时间，不覆盖付款信息或匹配状态。
// 历史未匹配流水被重新扫描后，进程即使在认单前退出，对账仍能按最近补录时间恢复。
func SaveChainTransfers(list []ChainTransfer) error {
	if len(list) == 0 {
		return nil
	}

	return Db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "network"}, {Name: "tx_hash"}, {Name: "event_index"}},
		DoUpdates: clause.Assignments(map[string]any{"updated_at": time.Now()}),
	}).CreateInBatches(list, 100).Error
}

var ErrChainTransferAlreadyMatched = errors.New("chain transfer has already been matched")

func ChainTransferKey(network, txHash string, eventIndex int) string {
	return fmt.Sprintf("%s:%s:%d", network, txHash, eventIndex)
}

// MatchedChainTransferKeys 批量识别已消费的事件，重复回放不发送非订单到账通知。
func MatchedChainTransferKeys(events []ChainTransfer) (map[string]struct{}, error) {
	keys := make(map[string]struct{})
	if len(events) == 0 {
		return keys, nil
	}
	networkSet, hashSet := make(map[string]struct{}), make(map[string]struct{})
	for _, event := range events {
		networkSet[event.Network] = struct{}{}
		hashSet[event.TxHash] = struct{}{}
	}
	networks, hashes := make([]string, 0, len(networkSet)), make([]string, 0, len(hashSet))
	for network := range networkSet {
		networks = append(networks, network)
	}
	for hash := range hashSet {
		hashes = append(hashes, hash)
	}
	for start := 0; start < len(hashes); start += 500 {
		var rows []ChainTransfer
		if err := Db.Select("network", "tx_hash", "event_index").
			Where("match_status = ? and network in (?) and tx_hash in (?)", ChainTransferMatched, networks, hashes[start:min(start+500, len(hashes))]).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			keys[ChainTransferKey(row.Network, row.TxHash, row.EventIndex)] = struct{}{}
		}
	}
	return keys, nil
}

// MatchChainTransfer 原子领取一笔未匹配入账并更新可收款订单。
// 流水或订单已被其他 worker 领取时不修改任何一方，重复扫描不能让同一付款认第二单。
func MatchChainTransfer(o *Order, network, txHash string, eventIndex, blockNum int, from, to string, at time.Time, amount decimal.Decimal) (bool, error) {
	claimed := false
	updated := *o
	err := Db.Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&ChainTransfer{}).
			Where("network = ? and tx_hash = ? and event_index = ? and match_status = ? and order_id = 0", network, txHash, eventIndex, ChainTransferUnmatched).
			Where("to_address = ? and trade_type = ? and amount = ? and block_time = ? and block_num = ?", to, o.TradeType, amount.String(), at, blockNum).
			Updates(map[string]any{"match_status": ChainTransferMatched, "order_id": o.ID})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			var existing ChainTransfer
			if err := tx.Select("match_status").Where("network = ? and tx_hash = ? and event_index = ?", network, txHash, eventIndex).
				Limit(1).Find(&existing).Error; err != nil {
				return err
			}
			if existing.MatchStatus == ChainTransferMatched {
				return ErrChainTransferAlreadyMatched
			}
			return nil
		}
		if err := updated.markConfirming(tx, blockNum, from, txHash, at, amount); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if errors.Is(err, ErrOrderNotReceivable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if claimed {
		*o = updated
	}
	return claimed, nil
}

// UnmatchedChainTransfers 指定时间之后付款或补录且仍未匹配到订单的入账。
func UnmatchedChainTransfers(since time.Time, limit int) []ChainTransfer {
	rows, _ := UnmatchedChainTransfersPage(since, 0, limit)

	return rows
}

// UnmatchedChainTransfersPage 也回查近期补录的旧流水；ID 翻页让最早的无订单流水不阻塞后续付款。
func UnmatchedChainTransfersPage(since time.Time, afterID int64, limit int) ([]ChainTransfer, error) {
	rows := make([]ChainTransfer, 0)
	err := Db.Where("match_status = ? and id > ? and (block_time >= ? or created_at >= ? or updated_at >= ?)", ChainTransferUnmatched, afterID, since, since, since).
		Order("id asc").Limit(limit).Find(&rows).Error
	return rows, err
}

func CountUnmatchedChainTransfers(since time.Time) int64 {
	var count int64
	Db.Model(&ChainTransfer{}).Where("match_status = ? and (block_time >= ? or created_at >= ? or updated_at >= ?)", ChainTransferUnmatched, since, since, since).Count(&count)

	return count
}
