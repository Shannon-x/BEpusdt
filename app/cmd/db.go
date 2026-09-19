package cmd

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/urfave/cli/v3"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/model/migration"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DB 数据库工具：在 SQLite / MySQL / PostgreSQL 之间整库复制，用于换库或从旧版 MySQL 部署迁移
var DB = &cli.Command{
	Name:  "db",
	Usage: "数据库工具：在 SQLite / MySQL / PostgreSQL 之间迁移数据",
	Commands: []*cli.Command{
		{
			Name:  "copy",
			Usage: "把全部数据从源库复制到目标库（目标库自动建表、按主键去重，可重复执行）。请先停止运行中的 bepusdt。",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "from-sqlite", Usage: "源 SQLite 文件路径"},
				&cli.StringFlag{Name: "from-mysql", Usage: "源 MySQL DSN"},
				&cli.StringFlag{Name: "from-postgres", Usage: "源 PostgreSQL DSN"},
				&cli.StringFlag{Name: "to-sqlite", Usage: "目标 SQLite 文件路径"},
				&cli.StringFlag{Name: "to-mysql", Usage: "目标 MySQL DSN"},
				&cli.StringFlag{Name: "to-postgres", Usage: "目标 PostgreSQL DSN"},
				&cli.IntFlag{Name: "batch", Usage: "每批复制的行数", Value: 500},
			},
			Action: dbCopy,
		},
	},
}

func pickDB(c *cli.Command, prefix string) (kind, dsn string, err error) {
	candidates := map[string]string{
		model.DriverSQLite:   c.String(prefix + "-sqlite"),
		model.DriverMySQL:    c.String(prefix + "-mysql"),
		model.DriverPostgres: c.String(prefix + "-postgres"),
	}
	for k, v := range candidates {
		if v == "" {
			continue
		}
		if kind != "" {
			return "", "", fmt.Errorf("--%s-* 只能指定一个数据库", prefix)
		}
		kind, dsn = k, v
	}
	if kind == "" {
		return "", "", fmt.Errorf("请通过 --%s-sqlite / --%s-mysql / --%s-postgres 指定数据库", prefix, prefix, prefix)
	}

	return kind, dsn, nil
}

func dbCopy(ctx context.Context, c *cli.Command) error {
	srcKind, srcDSN, err := pickDB(c, "from")
	if err != nil {
		return err
	}
	dstKind, dstDSN, err := pickDB(c, "to")
	if err != nil {
		return err
	}
	if srcKind == dstKind && srcDSN == dstDSN {
		return fmt.Errorf("源库与目标库相同")
	}

	src, err := model.Open(srcKind, srcDSN)
	if err != nil {
		return fmt.Errorf("打开源库失败：%w", err)
	}
	dst, err := model.Open(dstKind, dstDSN)
	if err != nil {
		return fmt.Errorf("打开目标库失败：%w", err)
	}

	fmt.Printf("源库：%s  →  目标库：%s\n", srcKind, dstKind)
	if _, err := migration.Run(dst, model.Models()); err != nil {
		return fmt.Errorf("目标库建表失败：%w", err)
	}

	batch := c.Int("batch")
	if batch <= 0 {
		batch = 500
	}

	start := time.Now()
	for _, m := range model.Models() {
		n, err := copyTable(src, dst, m, batch)
		if err != nil {
			return err
		}
		stmt := &gorm.Statement{DB: dst}
		_ = stmt.Parse(m)
		fmt.Printf("  %-22s %d 行\n", stmt.Schema.Table, n)
	}

	// 迁移记录一并复制，避免目标库重复执行已在源库执行过的迁移
	ids := migration.Applied(src)
	for _, id := range ids {
		dst.Exec("INSERT INTO "+migration.TableName+" (id) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM "+migration.TableName+" WHERE id = ?)", id, id)
	}

	if dstKind == model.DriverPostgres {
		resetPostgresSequences(dst)
	}

	fmt.Printf("完成，用时 %s。启动新库前请确认 SQLITE / MYSQL_DSN / POSTGRESQL_DSN 已指向目标库。\n", time.Since(start).Round(time.Millisecond))

	return nil
}

// copyTable 按主键顺序分批读取源表并以"冲突忽略"方式写入目标表，保留原主键
func copyTable(src, dst *gorm.DB, m any, batch int) (int64, error) {
	stmt := &gorm.Statement{DB: src}
	if err := stmt.Parse(m); err != nil {
		return 0, err
	}
	orderBy := "id"
	if stmt.Schema.PrioritizedPrimaryField != nil {
		orderBy = stmt.Schema.PrioritizedPrimaryField.DBName
	}

	sliceType := reflect.SliceOf(reflect.TypeOf(m).Elem())
	rows := reflect.New(sliceType).Interface()

	var total int64
	res := src.Model(m).Order(orderBy).FindInBatches(rows, batch, func(tx *gorm.DB, _ int) error {
		if reflect.ValueOf(rows).Elem().Len() == 0 {
			return nil
		}
		if err := dst.Clauses(clause.OnConflict{DoNothing: true}).Create(rows).Error; err != nil {
			return err
		}
		total += tx.RowsAffected

		return nil
	})
	if res.Error != nil {
		return total, fmt.Errorf("复制表 %s 失败：%w", stmt.Schema.Table, res.Error)
	}

	return total, nil
}

// resetPostgresSequences 显式写入主键后，把 PostgreSQL 自增序列推进到当前最大值
func resetPostgresSequences(dst *gorm.DB) {
	for _, m := range model.Models() {
		stmt := &gorm.Statement{DB: dst}
		if err := stmt.Parse(m); err != nil || stmt.Schema.PrioritizedPrimaryField == nil || stmt.Schema.PrioritizedPrimaryField.DBName != "id" {
			continue
		}
		table := stmt.Schema.Table
		dst.Exec(fmt.Sprintf("SELECT setval(pg_get_serial_sequence('%s', 'id'), COALESCE((SELECT MAX(id) FROM %s), 1))", table, table))
	}
}
