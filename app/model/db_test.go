package model

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func openTestDB(t *testing.T) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "model.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	Db = db
	if err := AutoMigrate(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeMySQLDSNFillsRequiredParams(t *testing.T) {
	got, err := NormalizeMySQLDSN("user:pass@tcp(127.0.0.1:3306)/bepusdt")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"parseTime=true", "charset=utf8mb4", "loc=Local", "timeout=5s", "readTimeout=30s", "writeTimeout=30s"} {
		if !strings.Contains(got, want) {
			t.Fatalf("normalized DSN must contain %s, got %s", want, got)
		}
	}

	// 用户显式指定的参数保留
	got, err = NormalizeMySQLDSN("user:pass@tcp(db:3306)/bepusdt?charset=utf8&loc=UTC&timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "charset=utf8&") && !strings.Contains(got, "charset=utf8") || strings.Contains(got, "charset=utf8mb4") {
		t.Fatalf("explicit charset must be kept, got %s", got)
	}
	// 显式 loc=UTC 是驱动默认值，FormatDSN 会省略；关键是不能被改成 Local
	if strings.Contains(got, "loc=Local") || !strings.Contains(got, "timeout=1s") {
		t.Fatalf("explicit loc/timeout must be kept, got %s", got)
	}

	if _, err := NormalizeMySQLDSN("not a dsn"); err == nil {
		t.Fatal("invalid DSN must be rejected")
	}
}

func TestOpenRejectsUnknownDriver(t *testing.T) {
	if _, err := Open("oracle", "x"); err == nil {
		t.Fatal("unknown driver must be rejected")
	}
}

func TestFillDefaultConfReportsAddedKeysAndUpgradeCheck(t *testing.T) {
	openTestDB(t)

	added := FillDefaultConf()
	if len(added) != len(defaultConf) {
		t.Fatalf("fresh database must receive every default, got %d of %d", len(added), len(defaultConf))
	}
	if again := FillDefaultConf(); len(again) != 0 {
		t.Fatalf("second fill must add nothing, got %v", again)
	}
	if missing := MissingDefaultConf(); len(missing) != 0 {
		t.Fatalf("nothing should be missing, got %v", missing)
	}

	// 删除一个配置项后应被识别为缺失并补齐
	Db.Where("k = ?", BlockBatchSize).Delete(&Conf{})
	if missing := MissingDefaultConf(); len(missing) != 1 || missing[0] != BlockBatchSize {
		t.Fatalf("deleted key must be reported missing, got %v", missing)
	}
	if added := FillDefaultConf(); len(added) != 1 || added[0] != BlockBatchSize {
		t.Fatalf("fill must restore exactly the missing key, got %v", added)
	}

	// 版本记录：首次运行不算升级，版本变化才算
	if prev, upgraded := UpgradeCheck("v1.25.0"); prev != "" || upgraded {
		t.Fatalf("first run must not count as upgrade, got prev=%q upgraded=%v", prev, upgraded)
	}
	if prev, upgraded := UpgradeCheck("v1.25.0"); upgraded || prev != "v1.25.0" {
		t.Fatalf("same version must not count as upgrade, got prev=%q upgraded=%v", prev, upgraded)
	}
	if prev, upgraded := UpgradeCheck("v1.26.0"); !upgraded || prev != "v1.25.0" {
		t.Fatalf("version change must count as upgrade, got prev=%q upgraded=%v", prev, upgraded)
	}
	if GetK(SystemVersion) != "v1.26.0" {
		t.Fatal("current version must be stored")
	}
}
