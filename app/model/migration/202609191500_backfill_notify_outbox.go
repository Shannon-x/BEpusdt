package migration

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

// 202609191500 - 把升级前尚未回调成功的订单补登记到回调 outbox，避免升级瞬间丢失待重试的回调
func m202609191500BackfillNotifyOutbox() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202609191500_backfill_notify_outbox",
		Migrate: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable("bep_order") || !tx.Migrator().HasTable("bep_notify_outbox") {
				return nil
			}

			return tx.Exec(`INSERT INTO bep_notify_outbox (order_id, trade_id, status, attempts, next_retry_at, created_at, updated_at)
SELECT o.id, o.trade_id, 'pending', o.notify_num, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM bep_order o
WHERE o.status = 2 AND o.notify_state = 0
  AND NOT EXISTS (SELECT 1 FROM bep_notify_outbox n WHERE n.order_id = o.id)`).Error
		},
		Rollback: func(tx *gorm.DB) error {
			return nil
		},
	}
}
