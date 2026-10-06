package model

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestScanJobUpgradeAddsResumeCheckpointWithoutLosingLegacyJobs(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "legacy-scan.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	previousDB := Db
	Db = db
	t.Cleanup(func() {
		Db = previousDB
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	})
	// v1.26.6 原有任务表没有 next_height；真实升级执行完整 AutoMigrate。
	legacyDDL := `CREATE TABLE bep_scan_job (
		id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
		network VARCHAR(20) NOT NULL,
		from_height INTEGER NOT NULL,
		to_height INTEGER NOT NULL,
		kind VARCHAR(16) NOT NULL,
		status VARCHAR(16) NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		last_error VARCHAR(512) NOT NULL DEFAULT '',
		next_retry_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	)`
	// GORM 的旧版建表语句为单行；SQLite migrator 按这份真实格式解析追加列。
	if err := db.Exec(strings.Join(strings.Fields(legacyDDL), " ")).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := db.Exec(`INSERT INTO bep_scan_job
		(id,network,from_height,to_height,kind,status,attempts,last_error,next_retry_at,created_at,updated_at)
		VALUES (77,'ethereum',100,10100,'gap','deferred',3,'legacy failure',?,?,?)`, now, now, now).Error; err != nil {
		t.Fatal(err)
	}
	if err := AutoMigrate(); err != nil {
		t.Fatal(err)
	}
	job, found := GetScanJob(77)
	if !found || job.Network != "ethereum" || job.FromHeight != 100 || job.ToHeight != 10100 || job.NextHeight != 0 || job.Attempts != 3 || job.LastError != "legacy failure" || job.Status != ScanJobStatusDeferred {
		t.Fatalf("upgrade must preserve legacy recovery evidence and default checkpoint to zero, got %+v found=%v", job, found)
	}
	if n, err := RetryScanJobs("ethereum", []string{ScanJobStatusDeferred}, job.ID); err != nil || n != 1 {
		t.Fatalf("legacy deferred job must be eligible for recovery, n=%d err=%v", n, err)
	}
	if err := job.MarkRunning(); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&job).Updates(map[string]any{"next_height": 5100, "status": ScanJobStatusPending}).Error; err != nil {
		t.Fatal(err)
	}
	checkpoint, _ := GetScanJob(job.ID)
	if checkpoint.NextHeight != 5100 {
		t.Fatalf("checkpoint write must precede repeated migration, got %+v", checkpoint)
	}
	// 再次迁移模拟下一次启动：进度不得回退或覆盖最初的恢复范围。
	if err := AutoMigrate(); err != nil {
		t.Fatal(err)
	}
	resumed, _ := GetScanJob(job.ID)
	if resumed.NextHeight != 5100 || resumed.FromHeight != 100 || resumed.ToHeight != 10100 || resumed.LastError != "legacy failure" {
		t.Fatalf("checkpoint and original recovery evidence must survive repeated migration, got %+v", resumed)
	}
}

func TestScanJobClaimAndManualRetryRestrictStatusTransitions(t *testing.T) {
	openTestDB(t)
	job, err := CreateScanJob("ethereum", 1, 2, ScanJobKindNative, ScanJobStatusPending, "original failure", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	other := job
	if err := job.MarkRunning(); err != nil {
		t.Fatal(err)
	}
	if err := other.MarkRunning(); err == nil {
		t.Fatal("only one dispatcher may claim a pending job")
	}
	if _, err := RetryScanJobs("ethereum", []string{ScanJobStatusRunning}, job.ID); err == nil {
		t.Fatal("manual retry must not reset currently running jobs")
	}
	if n, err := RetryScanJobs("ethereum", []string{ScanJobStatusFailed, ScanJobStatusDeferred}, job.ID); err != nil || n != 0 {
		t.Fatalf("eligible-status filter must protect running jobs, n=%d err=%v", n, err)
	}
	if err := Db.Model(&job).Updates(map[string]any{"status": ScanJobStatusFailed, "attempts": 20, "next_height": 2}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := RetryScanJobs("ethereum", []string{ScanJobStatusFailed}, job.ID); err != nil || n != 1 {
		t.Fatalf("failed native recovery job must become pending, n=%d err=%v", n, err)
	}
	got, _ := GetScanJob(job.ID)
	if got.Status != ScanJobStatusPending || got.Attempts != 0 || got.NextHeight != 2 || got.LastError != "original failure" || got.Kind != ScanJobKindNative {
		t.Fatalf("manual retry must preserve checkpoint, error and native recovery kind, got %+v", got)
	}
}
