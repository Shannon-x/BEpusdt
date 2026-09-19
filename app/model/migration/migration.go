package migration

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

const TableName = "bep_migration"

var migrations = []*gormigrate.Migration{
	m202607081430DropOrderTradeTypeReselect(),
	m202609191500BackfillNotifyOutbox(),
}

// Run 自动迁移表结构并执行版本化迁移；返回本次新建的数据表名，便于升级时在日志中确认
func Run(db *gorm.DB, initModels []any) ([]string, error) {
	created := make([]string, 0)
	for _, m := range initModels {
		if db.Migrator().HasTable(m) {
			continue
		}
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(m); err == nil {
			created = append(created, stmt.Schema.Table)
		}
	}

	if err := db.AutoMigrate(initModels...); err != nil {
		return nil, err
	}

	options := &gormigrate.Options{TableName: TableName}

	// 旧版升级/全新安装，构建迁移表
	if !db.Migrator().HasTable(TableName) {
		if err := db.Exec("CREATE TABLE " + TableName + " (id VARCHAR(255) PRIMARY KEY)").Error; err != nil {
			return nil, err
		}
	}

	m := gormigrate.New(db, options, migrations)

	return created, m.Migrate()
}

// Applied 已执行的迁移 ID
func Applied(db *gorm.DB) []string {
	ids := make([]string, 0)
	if !db.Migrator().HasTable(TableName) {
		return ids
	}
	db.Table(TableName).Order("id asc").Pluck("id", &ids)

	return ids
}
