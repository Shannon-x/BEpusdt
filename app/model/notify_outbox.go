package model

import (
	"time"

	"github.com/spf13/cast"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 回调 outbox：订单标记成功与"待回调"事件在同一事务内落库，发送由独立任务负责。
// 商户系统暂时不可用不影响认单，进程重启也不会丢失成功事件；每次发送的响应状态、响应摘要与下次重试时间都有记录。

const (
	NotifyOutboxPending = "pending" // 等待发送 / 重试
	NotifyOutboxSent    = "sent"    // 商户已确认
	NotifyOutboxDead    = "dead"    // 重试耗尽，需人工重发

	NotifyRetryMaxDelay = 6 * time.Hour
)

type NotifyOutbox struct {
	Id
	OrderID        int64      `gorm:"column:order_id;not null;uniqueIndex;comment:订单ID" json:"order_id"`
	TradeId        string     `gorm:"column:trade_id;type:varchar(128);not null;default:'';comment:本地订单号" json:"trade_id"`
	Status         string     `gorm:"column:status;type:varchar(16);not null;default:'pending';index:idx_notify_outbox_due,priority:1;comment:pending/sent/dead" json:"status"`
	Attempts       int        `gorm:"column:attempts;not null;default:0;comment:已发送次数" json:"attempts"`
	LastHttpStatus int        `gorm:"column:last_http_status;not null;default:0;comment:最后一次响应状态码" json:"last_http_status"`
	LastResponse   string     `gorm:"column:last_response;type:varchar(512);not null;default:'';comment:最后一次响应体摘要" json:"last_response"`
	LastError      string     `gorm:"column:last_error;type:varchar(512);not null;default:'';comment:最后一次失败原因" json:"last_error"`
	NextRetryAt    time.Time  `gorm:"column:next_retry_at;not null;index:idx_notify_outbox_due,priority:2;comment:下次发送时间" json:"next_retry_at"`
	SentAt         *time.Time `gorm:"column:sent_at;comment:商户确认时间" json:"sent_at"`
	AutoTimeAt
}

func (NotifyOutbox) TableName() string {
	return "bep_notify_outbox"
}

// EnqueueNotify 登记待回调事件，已存在则忽略（幂等）
func EnqueueNotify(db *gorm.DB, orderID int64, tradeId string) error {
	row := NotifyOutbox{OrderID: orderID, TradeId: tradeId, Status: NotifyOutboxPending, NextRetryAt: time.Now()}

	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error
}

// SetSuccessWithNotify 订单标记成功并登记回调事件，同一事务
func (o *Order) SetSuccessWithNotify() error {
	return Db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(o).Updates(map[string]any{"status": OrderStatusSuccess}).Error; err != nil {
			return err
		}
		o.Status = OrderStatusSuccess

		return EnqueueNotify(tx, o.ID, o.TradeId)
	})
}

func GetNotifyOutbox(orderID int64) (NotifyOutbox, bool) {
	var row NotifyOutbox
	res := Db.Where("order_id = ?", orderID).Limit(1).Find(&row)

	return row, res.Error == nil && res.RowsAffected > 0
}

// DueNotifyOutbox 到期待发送的回调
func DueNotifyOutbox(limit int) []NotifyOutbox {
	rows := make([]NotifyOutbox, 0)
	Db.Where("status = ? and next_retry_at <= ?", NotifyOutboxPending, time.Now()).
		Order("id asc").Limit(limit).Find(&rows)

	return rows
}

func (n *NotifyOutbox) MarkSent(httpStatus int, body string) error {
	now := time.Now()
	n.Status = NotifyOutboxSent
	n.Attempts++
	n.LastHttpStatus = httpStatus
	n.LastResponse = truncate(body, 512)
	n.LastError = ""
	n.SentAt = &now

	return Db.Model(n).Updates(map[string]any{
		"status":           n.Status,
		"attempts":         n.Attempts,
		"last_http_status": n.LastHttpStatus,
		"last_response":    n.LastResponse,
		"last_error":       "",
		"sent_at":          now,
	}).Error
}

// MarkFailed 记一次失败并安排重试；达到 notify_max_retry 次后标记 dead。返回是否已 dead
func (n *NotifyOutbox) MarkFailed(httpStatus int, body, reason string) (bool, error) {
	n.Attempts++
	n.LastHttpStatus = httpStatus
	n.LastResponse = truncate(body, 512)
	n.LastError = truncate(reason, 512)
	n.Status = NotifyOutboxPending
	n.NextRetryAt = time.Now().Add(NotifyRetryDelay(n.Attempts))
	if n.Attempts >= NotifyMaxRetryNum() {
		n.Status = NotifyOutboxDead
	}

	return n.Status == NotifyOutboxDead, Db.Model(n).Updates(map[string]any{
		"status":           n.Status,
		"attempts":         n.Attempts,
		"last_http_status": n.LastHttpStatus,
		"last_response":    n.LastResponse,
		"last_error":       n.LastError,
		"next_retry_at":    n.NextRetryAt,
	}).Error
}

func (n *NotifyOutbox) MarkDead(reason string) error {
	n.Status = NotifyOutboxDead
	n.LastError = truncate(reason, 512)

	return Db.Model(n).Updates(map[string]any{"status": n.Status, "last_error": n.LastError}).Error
}

// NotifyRetryDelay 第 attempts 次失败后的等待：1m, 2m, 4m, ... 上限 6h
func NotifyRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	delay := time.Minute
	for i := 1; i < attempts && delay < NotifyRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > NotifyRetryMaxDelay {
		delay = NotifyRetryMaxDelay
	}

	return delay
}

// NotifyMaxRetryNum 回调最大尝试次数，来自 notify_max_retry 配置
func NotifyMaxRetryNum() int {
	n := cast.ToInt(GetC(NotifyMaxRetry))
	if n <= 0 {
		n = cast.ToInt(defaultConf[NotifyMaxRetry])
	}
	if n <= 0 {
		n = 10
	}

	return n
}

// NotifyOutboxStats 待发送数量、最早待发送的登记时间、dead 数量
func NotifyOutboxStats() (pending int64, oldest *time.Time, dead int64) {
	Db.Model(&NotifyOutbox{}).Where("status = ?", NotifyOutboxPending).Count(&pending)
	Db.Model(&NotifyOutbox{}).Where("status = ?", NotifyOutboxDead).Count(&dead)

	var first NotifyOutbox
	res := Db.Where("status = ?", NotifyOutboxPending).Order("id asc").Limit(1).Find(&first)
	if res.Error == nil && res.RowsAffected > 0 && first.CreatedAt != nil {
		t := first.CreatedAt.Time()
		oldest = &t
	}

	return pending, oldest, dead
}
