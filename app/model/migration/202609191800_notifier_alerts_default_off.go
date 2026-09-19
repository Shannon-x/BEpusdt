package migration

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

// 202609191800 - 系统告警默认关闭
//
// notifier_alerts 在 v1.26.5 首次引入时默认 important，实际使用中这些系统告警偏吵，
// v1.26.6 起默认关闭（只记日志）。这里把仍是初始默认值 important 的实例一并调整为 off；
// 已手动改成 all 或 off 的实例不受影响。
func m202609191800NotifierAlertsDefaultOff() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202609191800_notifier_alerts_default_off",
		Migrate: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable("bep_conf") {
				return nil
			}

			return tx.Exec("UPDATE bep_conf SET v = 'off' WHERE k = 'notifier_alerts' AND v = 'important'").Error
		},
		Rollback: func(tx *gorm.DB) error {
			return nil
		},
	}
}
