package model

import (
	"fmt"
	"time"

	"gorm.io/gorm/clause"
)

// 扫描持久化：连续游标 + 失败/回放任务。
// 游标只记录"已连续完成"的高度：N 成功后才推进到 N，即使 N+1、N+2 已成功也不会越过失败的 N；
// 放弃的区块会先写入任务表再允许游标越过，因此任何区块都不会静默丢失。

const (
	ScanJobKindAbandoned = "abandoned" // 重试耗尽放弃的区块
	ScanJobKindGap       = "gap"       // 链头跳跃 / 重启导致未扫描的区间
	ScanJobKindReplay    = "replay"    // 手动回放
	ScanJobKindNative    = "native"    // 原生币补扫，不重复扫描已完成的代币日志

	ScanJobStatusPending  = "pending"  // 等待自动重试
	ScanJobStatusRunning  = "running"  // 已入队执行中
	ScanJobStatusDone     = "done"     // 全部区块成功
	ScanJobStatusFailed   = "failed"   // 自动重试次数耗尽，需人工处理
	ScanJobStatusDeferred = "deferred" // 不自动重试，仅记录（大范围 gap）

	ScanJobMaxAttempts   = 20               // 任务级最大自动重试次数
	ScanJobStaleAfter    = 30 * time.Minute // running 超过该时长视为进程异常退出遗留
	ScanJobRetryBase     = 5 * time.Minute
	ScanJobRetryMaxDelay = 6 * time.Hour
)

type ScanCursor struct {
	Network   string    `gorm:"column:network;type:varchar(20);primaryKey" json:"network"`
	Height    int64     `gorm:"column:height;not null;default:0;comment:已连续扫描完成的最高高度" json:"height"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null" json:"updated_at"`
}

func (ScanCursor) TableName() string {
	return "bep_scan_cursor"
}

func GetScanCursor(network string) (int64, bool) {
	height, found, _ := LoadScanCursor(network)

	return height, found
}

// LoadScanCursor 区分首次启动（没有游标）和数据库读取失败，避免故障时从链头重新开始。
func LoadScanCursor(network string) (int64, bool, error) {
	var row ScanCursor
	res := Db.Where("network = ?", network).Limit(1).Find(&row)
	if res.Error != nil {
		return 0, false, res.Error
	}

	return row.Height, res.RowsAffected > 0, nil
}

func SaveScanCursor(network string, height int64) error {
	row := ScanCursor{Network: network, Height: height, UpdatedAt: time.Now()}

	return Db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "network"}},
		DoUpdates: clause.AssignmentColumns([]string{"height", "updated_at"}),
	}).Create(&row).Error
}

func AllScanCursors() []ScanCursor {
	rows := make([]ScanCursor, 0)
	Db.Order("network asc").Find(&rows)

	return rows
}

type ScanJob struct {
	Id
	Network     string    `gorm:"column:network;type:varchar(20);not null;index:idx_scan_job_due,priority:2;comment:网络" json:"network"`
	FromHeight  int64     `gorm:"column:from_height;not null;comment:起始高度" json:"from_height"`
	ToHeight    int64     `gorm:"column:to_height;not null;comment:结束高度(含)" json:"to_height"`
	NextHeight  int64     `gorm:"column:next_height;not null;default:0;comment:下次扫描起点，0表示from_height" json:"next_height"`
	Kind        string    `gorm:"column:kind;type:varchar(16);not null;comment:abandoned/gap/replay/native" json:"kind"`
	Status      string    `gorm:"column:status;type:varchar(16);not null;index:idx_scan_job_due,priority:1;comment:pending/running/done/failed/deferred" json:"status"`
	Attempts    int       `gorm:"column:attempts;not null;default:0;comment:已自动重试次数" json:"attempts"`
	LastError   string    `gorm:"column:last_error;type:varchar(512);not null;default:'';comment:最后一次失败原因" json:"last_error"`
	NextRetryAt time.Time `gorm:"column:next_retry_at;not null;index:idx_scan_job_due,priority:3;comment:下次重试时间" json:"next_retry_at"`
	AutoTimeAt
}

func (ScanJob) TableName() string {
	return "bep_scan_job"
}

func CreateScanJob(network string, from, to int64, kind, status, lastErr string, nextRetry time.Time) (ScanJob, error) {
	job := ScanJob{
		Network:     network,
		FromHeight:  from,
		ToHeight:    to,
		Kind:        kind,
		Status:      status,
		LastError:   truncate(lastErr, 512),
		NextRetryAt: nextRetry,
	}

	return job, Db.Create(&job).Error
}

func GetScanJob(id int64) (ScanJob, bool) {
	var job ScanJob
	res := Db.Where("id = ?", id).Limit(1).Find(&job)

	return job, res.Error == nil && res.RowsAffected > 0
}

// DueScanJobs 到期待重试的任务，按创建顺序
func DueScanJobs(limit int) []ScanJob {
	jobs := make([]ScanJob, 0)
	Db.Where("status = ? and next_retry_at <= ?", ScanJobStatusPending, time.Now()).
		Order("id asc").Limit(limit).Find(&jobs)

	return jobs
}

func RecentScanJobs(network string, limit int) []ScanJob {
	jobs := make([]ScanJob, 0)
	db := Db.Order("id desc").Limit(limit)
	if network != "" {
		db = db.Where("network = ?", network)
	}
	db.Find(&jobs)

	return jobs
}

// ScanJobCounts 各状态任务数量
func ScanJobCounts(network string) map[string]int64 {
	type row struct {
		Status string
		Total  int64
	}
	rows := make([]row, 0)
	db := Db.Model(&ScanJob{}).Select("status, count(*) as total").Group("status")
	if network != "" {
		db = db.Where("network = ?", network)
	}
	db.Scan(&rows)

	counts := make(map[string]int64, len(rows))
	for _, r := range rows {
		counts[r.Status] = r.Total
	}

	return counts
}

// ScanJobCountsAll 一次查询得到所有网络各状态的任务数量：network → status → count
func ScanJobCountsAll() map[string]map[string]int64 {
	type row struct {
		Network string
		Status  string
		Total   int64
	}
	rows := make([]row, 0)
	Db.Model(&ScanJob{}).Select("network, status, count(*) as total").Group("network, status").Scan(&rows)

	out := make(map[string]map[string]int64)
	for _, r := range rows {
		if out[r.Network] == nil {
			out[r.Network] = make(map[string]int64)
		}
		out[r.Network][r.Status] = r.Total
	}

	return out
}

func (j *ScanJob) MarkRunning() error {
	res := Db.Model(j).Where("status = ?", ScanJobStatusPending).
		Updates(map[string]any{"status": ScanJobStatusRunning})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("scan job #%d is no longer pending", j.ID)
	}
	j.Status = ScanJobStatusRunning

	return nil
}

func (j *ScanJob) MarkDone() error {
	j.Status = ScanJobStatusDone
	j.LastError = ""

	return Db.Model(j).Updates(map[string]any{"status": j.Status, "last_error": ""}).Error
}

// MarkFailed 记一次失败：未超过上限则按退避安排下次重试，否则标记为 failed 停止自动重试
func (j *ScanJob) MarkFailed(reason string) error {
	j.Attempts++
	j.LastError = truncate(reason, 512)
	j.Status = ScanJobStatusPending
	j.NextRetryAt = time.Now().Add(ScanJobRetryDelay(j.Attempts))
	if j.Attempts >= ScanJobMaxAttempts {
		j.Status = ScanJobStatusFailed
	}

	return Db.Model(j).Updates(map[string]any{
		"attempts":      j.Attempts,
		"last_error":    j.LastError,
		"status":        j.Status,
		"next_retry_at": j.NextRetryAt,
	}).Error
}

// ScanJobRetryDelay 任务级退避：5m, 10m, 20m, ... 上限 6h
func ScanJobRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	delay := ScanJobRetryBase
	for i := 1; i < attempts && delay < ScanJobRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > ScanJobRetryMaxDelay {
		delay = ScanJobRetryMaxDelay
	}

	return delay
}

// ResetStaleScanJobs 进程异常退出后遗留的 running 任务重新置为 pending，启动时调用
func ResetStaleScanJobs(olderThan time.Duration) int64 {
	n, _ := RecoverScanJobs(olderThan, nil)

	return n
}

// RecoverScanJobs 单进程启动时 olderThan=0，立即恢复上个进程的全部 running 任务。
// 周期调用排除当前进程已跟踪任务；只回收失去内存跟踪且超过期限的任务。
func RecoverScanJobs(olderThan time.Duration, activeIDs []int64) (int64, error) {
	db := Db.Model(&ScanJob{}).Where("status = ?", ScanJobStatusRunning)
	if olderThan > 0 {
		db = db.Where("updated_at < ?", time.Now().Add(-olderThan))
	}
	if len(activeIDs) > 0 {
		db = db.Where("id not in ?", activeIDs)
	}
	res := db.Updates(map[string]any{"status": ScanJobStatusPending, "next_retry_at": time.Now()})

	return res.RowsAffected, res.Error
}

// RetryScanJobs 人工恢复失败/跳过的任务，保留原始范围、分段进度和错误证据。
func RetryScanJobs(network string, statuses []string, id int64) (int64, error) {
	if len(statuses) == 0 {
		return 0, fmt.Errorf("retry requires failed or deferred status")
	}
	for _, status := range statuses {
		if status != ScanJobStatusFailed && status != ScanJobStatusDeferred {
			return 0, fmt.Errorf("status %s cannot be retried manually", status)
		}
	}
	if network == "" && id <= 0 {
		return 0, fmt.Errorf("retry requires network or job id")
	}
	db := Db.Model(&ScanJob{}).Where("status in ?", statuses)
	if network != "" {
		db = db.Where("network = ?", network)
	}
	if id > 0 {
		db = db.Where("id = ?", id)
	}
	res := db.Updates(map[string]any{"status": ScanJobStatusPending, "attempts": 0, "next_retry_at": time.Now()})

	return res.RowsAffected, res.Error
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}

	return s
}
