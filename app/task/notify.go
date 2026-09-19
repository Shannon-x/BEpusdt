package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/notifier"
	"github.com/v03413/bepusdt/app/task/notify"
)

// notifyInflight 正在回调中的订单，避免重试轮询在上一次回调尚未返回时重复触发同一订单
var notifyInflight sync.Map // orderID(int64) → struct{}

func init() {
	Register(Task{Duration: time.Second * 3, Callback: notifyRetry})
	Register(Task{Duration: time.Second * 30, Callback: notifyRoll})
}

// notifyRetry 从回调 outbox 取到期事件发送；每条事件的尝试次数、响应与下次重试时间都记录在表中
func notifyRetry(context.Context) {
	maxRetry := model.NotifyMaxRetryNum()
	for _, row := range model.DueNotifyOutbox(50) {
		if row.Attempts >= maxRetry { // 升级前遗留的超限订单
			_ = row.MarkDead("历史重试次数已达上限")

			continue
		}

		order, ok := model.GetOrderByID(row.OrderID)
		if !ok || order.Status != model.OrderStatusSuccess {
			_ = row.MarkDead("订单不存在或状态已非交易成功")

			continue
		}

		dispatchNotify(order)
	}
}

// dispatchNotify 异步执行商户回调：同一订单同一时刻只允许一次在途请求，商户已确认过的订单不再自动重发
func dispatchNotify(order model.Order) {
	if row, ok := model.GetNotifyOutbox(order.ID); ok && row.Status == model.NotifyOutboxSent {
		return
	}
	if _, busy := notifyInflight.LoadOrStore(order.ID, struct{}{}); busy {
		return
	}

	go func() {
		defer notifyInflight.Delete(order.ID)

		if err := notify.Handle(order); err != nil {
			log.Task.Warn(fmt.Sprintf("订单回调未成功(%s)：%s", order.TradeId, err.Error()))
		}
	}()
}

func notifyRoll(context.Context) {
	for _, o := range model.GetOrderByStatus(model.OrderStatusWaiting) {
		notify.Bepusdt(o)
	}
}

// notifyOrderSuccess 统一触发订单成功后的回调与订单通知。
func notifyOrderSuccess(order model.Order) {
	dispatchNotify(order)
	go notifier.Success(order)
}
