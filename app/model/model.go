package model

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	mysqldrv "github.com/go-sql-driver/mysql"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model/migration"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var Db *gorm.DB
var err error

// 数据库类型
const (
	DriverSQLite   = "sqlite"
	DriverMySQL    = "mysql"
	DriverPostgres = "postgres"
)

var dbDriver = DriverSQLite

type Id struct {
	ID int64 `gorm:"column:id;primaryKey;autoIncrement;not null;comment:主键ID" json:"id"`
}

type AutoTimeAt struct {
	CreatedAt *Datetime `gorm:"column:created_at;not null;comment:记录创建时间;index" json:"created_at"`
	UpdatedAt *Datetime `gorm:"column:updated_at;not null;comment:最后更新时间" json:"updated_at"`
}

// Driver 当前使用的数据库类型
func Driver() string {
	return dbDriver
}

// Models 全部数据表模型，迁移与数据库间复制共用
func Models() []any {
	return []any{
		&Wallet{},
		&Order{},
		&NotifyRecord{},
		&Conf{},
		&Rate{},
		&ScanCursor{},
		&ScanJob{},
		&ChainTransfer{},
		&NotifyOutbox{},
	}
}

// Init 初始化数据库：postgres > mysql > sqlite，随后自动迁移结构、补齐默认配置
func Init(sqlitePath, mysqlDSN, postgresDSN string) error {
	kind, dsn := DriverSQLite, sqlitePath
	switch {
	case postgresDSN != "":
		kind, dsn = DriverPostgres, postgresDSN
	case mysqlDSN != "":
		kind, dsn = DriverMySQL, mysqlDSN
	}

	db, err := Open(kind, dsn)
	if err != nil {
		return err
	}

	Db = db
	dbDriver = kind

	return bootstrap()
}

// Open 按类型打开数据库连接（不做迁移），供 Init 与数据库迁移工具复用
func Open(kind, dsn string) (*gorm.DB, error) {
	switch kind {
	case DriverSQLite:
		return openSqlite(dsn)
	case DriverMySQL:
		return openMySQL(dsn)
	case DriverPostgres:
		return openPostgres(dsn)
	}

	return nil, fmt.Errorf("不支持的数据库类型：%s", kind)
}

func openSqlite(path string) (*gorm.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("SQLite 数据库文件路径不能为空")
	}
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {

		return nil, fmt.Errorf("创建数据库目录失败：%w", err)
	}

	dsn := fmt.Sprintf("%s?cache=shared&mode=rwc"+
		"&_pragma=cache_size(-32000)"+ // 32MB 缓存，平衡内存占用
		"&_pragma=journal_mode(WAL)"+
		"&_pragma=busy_timeout(8000)"+ // 8 秒超时，兼顾慢速磁盘
		"&_pragma=synchronous(NORMAL)"+ // NORMAL 模式，性能与安全平衡
		"&_pragma=wal_autocheckpoint(1500)", // 适中的 checkpoint 频率
		path)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {

		return nil, fmt.Errorf("数据库初始化失败：%w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {

		return nil, fmt.Errorf("获取数据库连接失败：%w", err)
	}
	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetMaxIdleConns(3)
	sqlDB.SetConnMaxLifetime(0)

	return db, nil
}

// NormalizeMySQLDSN 补齐 MySQL DSN 必需参数：parseTime（时间字段解析）、utf8mb4、本地时区、超时
func NormalizeMySQLDSN(dsn string) (string, error) {
	cfg, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("MySQL DSN 格式错误：%w（示例：user:password@tcp(127.0.0.1:3306)/bepusdt?charset=utf8mb4&parseTime=True&loc=Local）", err)
	}

	cfg.ParseTime = true
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	lower := strings.ToLower(dsn)
	if !strings.Contains(lower, "charset=") { // 驱动把 charset 存在独立字段，按原始 DSN 判断是否已指定
		cfg.Params["charset"] = "utf8mb4"
	}
	if !strings.Contains(lower, "loc=") {
		cfg.Loc = time.Local
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}

	return cfg.FormatDSN(), nil
}

// openMySQL 兼容 MySQL 5.7+ / 8.x、MariaDB 10.x、TiDB 等 MySQL 协议数据库
func openMySQL(dsn string) (*gorm.DB, error) {
	normalized, err := NormalizeMySQLDSN(dsn)
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(mysql.New(mysql.Config{DSN: normalized, DefaultStringSize: 256}), &gorm.Config{})
	if err != nil {

		return nil, fmt.Errorf("MySQL 连接失败：%w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {

		return nil, fmt.Errorf("获取数据库连接失败：%w", err)
	}
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(time.Hour)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)

	return db, nil
}

func openPostgres(dsn string) (*gorm.DB, error) {
	// 首次启动可能出现 SLOW SQL 告警，这是由于连接池首次连接预热引起的，后续连接将正常
	db, err := gorm.Open(postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true}), &gorm.Config{})
	if err != nil {

		return nil, fmt.Errorf("PostgreSQL 连接失败：%w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {

		return nil, err
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)

	return db, nil
}

// bootstrap 迁移结构、初始化 / 补齐配置、加载配置缓存
func bootstrap() error {
	if err := AutoMigrate(); err != nil {

		return fmt.Errorf("数据库结构迁移失败：%w", err)
	}

	var count int64
	Db.Model(&Conf{}).Count(&count)
	if count == 0 {
		ConfInit()
	}

	if added := FillDefaultConf(); len(added) > 0 {
		keys := make([]string, 0, len(added))
		for _, k := range added {
			keys = append(keys, string(k))
		}
		log.Info("配置升级：新增配置项并写入默认值 " + strings.Join(keys, ", "))
	}
	RefreshC()

	return nil
}

// AutoMigrate 迁移全部表结构；新建的表会记录到日志，便于升级时确认
func AutoMigrate() error {
	created, err := migration.Run(Db, Models())
	if err != nil {
		return err
	}
	if len(created) > 0 {
		log.Info("数据库升级：新建数据表 " + strings.Join(created, ", "))
	}

	return nil
}

func Close() {
	if Db == nil {

		return
	}

	sqlDB, err := Db.DB()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("数据库资源句柄获取异常：%s", err.Error()))

		return
	}

	if err := sqlDB.Close(); err != nil {

		_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("数据库资源关闭错误：%s", err.Error()))
	}
}
