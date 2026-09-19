package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cast"
	"github.com/v03413/bepusdt/app"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/notifier"
	"github.com/v03413/bepusdt/app/utils"

	"github.com/v03413/go-cache"
	"gorm.io/gorm"
)

type EpNotify struct {
	TradeId            string  `json:"trade_id"`             //  本地订单号
	OrderId            string  `json:"order_id"`             //  客户交易id
	Amount             float64 `json:"amount"`               //  订单金额 CNY
	ActualAmount       string  `json:"actual_amount"`        //  USDT 交易数额
	Token              string  `json:"token"`                //  收款钱包地址
	BlockTransactionId string  `json:"block_transaction_id"` // 区块id
	Signature          string  `json:"signature"`            // 签名
	Status             int     `json:"status"`               //  1：等待支付，2：支付成功，3：订单超时
}

// Handle 执行一次商户回调并把结果写入回调 outbox：成功标记 sent，失败记录状态码 / 响应摘要并安排退避重试
func Handle(order model.Order) error {
	if order.Status != model.OrderStatusSuccess {

		return errors.New("订单未支付 无法回调")
	}

	// 手动补单 / 手动重发等入口可能没有登记过事件，这里幂等补登记
	if err := model.EnqueueNotify(model.Db, order.ID, order.TradeId); err != nil {
		log.Warn("enqueue notify outbox error:", err.Error())
	}

	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var status int
	var body string
	var err error
	if order.ApiType == model.OrderApiTypeEpay {
		status, body, err = epay(ctx, order)
	} else {
		status, body, err = epusdt(ctx, order)
	}

	if err != nil {
		markNotifyFail(order, status, body, err.Error())

		return err
	}

	markNotifySuccess(order, status, body)
	log.Info("订单回调成功：", order.TradeId)

	return nil
}

// epay 易支付回调；返回商户响应状态码与响应体摘要，不做任何状态落库
func epay(ctx context.Context, order model.Order) (int, string, error) {
	var client = http.Client{Timeout: time.Second * 5}
	var notifyUrl = fmt.Sprintf("%s?%s", order.NotifyUrl, order.BuildNotifyParams())

	postReq, err := http.NewRequestWithContext(ctx, "GET", notifyUrl, nil)
	if err != nil {
		return 0, "", err
	}

	postReq.Header.Set("Powered-By", "https://github.com/v03413/bepusdt")
	resp, err := client.Do(postReq)
	if err != nil {
		return 0, "", err
	}

	defer resp.Body.Close()
	all, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return resp.StatusCode, "", fmt.Errorf("io.ReadAll(resp.Body) Error: %v", err)
	}

	if resp.StatusCode != 200 {
		return resp.StatusCode, string(all), fmt.Errorf("商户系统返回状态码错误：%d（必须是200）", resp.StatusCode)
	}

	var bodyStr = strings.ToLower(strings.TrimSpace(string(all)))

	// 判断是否包含 success 或 ok
	if !strings.Contains(bodyStr, "success") && !strings.Contains(bodyStr, "ok") {
		return resp.StatusCode, string(all), fmt.Errorf("商户系统必须响应 success 或 ok 才会认定回调成功，实际响应：%s", string(all))
	}

	return resp.StatusCode, string(all), nil
}

// epusdt Epusdt 协议回调；返回商户响应状态码与响应体摘要，不做任何状态落库
func epusdt(ctx context.Context, order model.Order) (int, string, error) {
	var data = make(map[string]interface{})
	var body = EpNotify{
		TradeId:            order.TradeId,
		OrderId:            order.OrderId,
		Amount:             cast.ToFloat64(order.Money),
		ActualAmount:       order.Amount,
		Token:              order.Address,
		BlockTransactionId: order.RefHash,
		Status:             order.Status,
	}
	var jsonBody, err = json.Marshal(body)
	if err != nil {
		return 0, "", err
	}

	if err = json.Unmarshal(jsonBody, &data); err != nil {
		return 0, "", err
	}

	// 签名
	body.Signature = utils.EpusdtSign(data, model.AuthToken())

	// 再次序列化
	jsonBody, err = json.Marshal(body)
	if err != nil {
		return 0, "", err
	}

	var client = http.Client{Timeout: time.Second * 5}
	var postReq, err2 = http.NewRequestWithContext(ctx, "POST", order.NotifyUrl, strings.NewReader(string(jsonBody)))
	if err2 != nil {
		return 0, "", err2
	}

	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("Powered-By", "https://github.com/v03413/bepusdt")
	postReq.Header.Set("User-Agent", "BEpusdt/"+app.Version)
	resp, err := client.Do(postReq)
	if err != nil {
		return 0, "", err
	}

	defer resp.Body.Close()
	all, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != 200 {
		return resp.StatusCode, string(all), fmt.Errorf("商户系统返回状态码错误：%d（必须是200）", resp.StatusCode)
	}

	return resp.StatusCode, string(all), nil
}

func Bepusdt(o model.Order) {
	if o.ApiType != model.OrderApiTypeEpusdt && o.ApiType != model.OrderApiTypeEpusdtOrder {

		return
	}

	var authToken = model.AuthToken()
	var client = &http.Client{Timeout: time.Second * 5}
	go func() {
		if err := deliverBepusdtStatusUpdate(model.Db, client, authToken, o); err != nil {
			log.Warn("notify BEpusdt Error:", err.Error())
		}
	}()
}

func deliverBepusdtStatusUpdate(db *gorm.DB, client *http.Client, authToken string, o model.Order) error {
	if client == nil {
		client = &http.Client{Timeout: time.Second * 5}
	}

	var current model.Order
	tx := db.Where("trade_id = ? and status = ?", o.TradeId, o.Status).Limit(1).Find(&current)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return nil
	}

	var key = fmt.Sprintf("bepusdt_notify_%d_%s", current.Status, current.TradeId)
	if _, ok := cache.Get(key); ok {
		return nil
	}

	cache.Set(key, true, time.Minute)

	var data = make(map[string]interface{})
	var body = EpNotify{
		TradeId:            current.TradeId,
		OrderId:            current.OrderId,
		Amount:             cast.ToFloat64(current.Money),
		ActualAmount:       current.Amount,
		Token:              current.Address,
		BlockTransactionId: current.RefHash,
		Status:             current.Status,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(jsonBody, &data); err != nil {
		return err
	}

	body.Signature = utils.EpusdtSign(data, authToken)

	jsonBody, _ = json.Marshal(body)
	req, err := http.NewRequest("POST", current.NotifyUrl, strings.NewReader(string(jsonBody)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Powered-By", "https://github.com/v03413/BEpusdt")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("resp.StatusCode != 200")
	}

	all, _ := io.ReadAll(resp.Body)
	log.Info(fmt.Sprintf("订单回调成功[%d]：%s %s", current.Status, current.TradeId, string(all)))

	return nil
}

// markNotifySuccess 订单回调状态置成功，outbox 标记 sent
func markNotifySuccess(o model.Order, httpStatus int, body string) {
	if err := o.SetNotifyState(model.OrderNotifyStateSucc); err != nil {
		log.Warn(fmt.Sprintf("订单回调状态更新失败(%v)：%v", o.TradeId, err))
	}
	if row, ok := model.GetNotifyOutbox(o.ID); ok {
		if err := row.MarkSent(httpStatus, body); err != nil {
			log.Warn(fmt.Sprintf("notify outbox mark sent error(%v)：%v", o.TradeId, err))
		}
	}
}

// markNotifyFail 记一次失败：订单回调次数 +1，outbox 记录状态码 / 响应 / 原因并安排退避重试；重试耗尽时告警
func markNotifyFail(o model.Order, httpStatus int, body, reason string) {
	log.Warn(fmt.Sprintf("订单回调失败(%v)：%s %v", o.TradeId, reason, o.SetNotifyState(model.OrderNotifyStateFail)))

	dead := false
	var row model.NotifyOutbox
	if r, ok := model.GetNotifyOutbox(o.ID); ok {
		row = r
		var err error
		if dead, err = row.MarkFailed(httpStatus, body, reason); err != nil {
			log.Warn(fmt.Sprintf("notify outbox mark failed error(%v)：%v", o.TradeId, err))
		}
	}

	notifier.NotifyFail(o, reason)
	if dead {
		notifier.Alert("订单回调重试耗尽",
			fmt.Sprintf("订单：%s\n已尝试 %d 次回调均失败，已停止自动重试。\n最后状态码：%d\n最后错误：%s\n请修复商户系统后在后台订单详情手动重发回调。", o.TradeId, row.Attempts, httpStatus, reason))
	}
}
