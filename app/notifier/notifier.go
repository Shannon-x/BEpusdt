package notifier

import (
	"sync"

	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

const (
	ChannelNone     = "none"
	ChannelWechat   = "wechat"
	ChannelTelegram = "telegram"
)

// 告警推送级别；无论哪一级，事件本身都会记入日志，这里只决定是否推送到通知渠道
const (
	AlertsOff       = "off"       // 全部不推送（默认）
	AlertsImportant = "important" // 仅推送影响收款的告警
	AlertsAll       = "all"       // 连同运维提示一起推送
)

type Notifier interface {
	Initialize(params string) error                             // 初始化
	Success(o model.Order)                                      // 交易成功通知
	NotifyFail(o model.Order, reason string)                    // 订单回调失败通知
	NonOrderTransfer(trans model.TronTransfer, wa model.Wallet) // 非订单交易通知
	TronResourceChange(res model.TronResource)                  // Tron 资源变动通知
	Welcome()                                                   // 程序启动时的欢迎信息
	Alert(title, text string)                                   // 系统告警（扫块停滞、区块放弃、队列拥堵等）
	Test() error                                                // 测试通知是否成功
}

// notifierMap 按渠道+参数缓存已初始化的通知器；Success / NotifyFail / Alert 都在各自的 goroutine 中调用，必须加锁
var notifierMap = make(map[string]Notifier)
var notifierMu sync.Mutex
var confKeys = []model.ConfKey{
	model.NotifierChannel,
	model.NotifierParams,
}

func NewNotifier(channel, params string) (Notifier, error) {
	var key = utils.Md5String(channel + params)

	notifierMu.Lock()
	defer notifierMu.Unlock()

	if n, ok := notifierMap[key]; ok {
		return n, nil
	}

	var notifier Notifier

	switch channel {
	case ChannelNone:
		notifier = &None{}
	case ChannelWechat:
		notifier = &Wechat{}
	case ChannelTelegram:
		notifier = &Telegram{}
	default:
		notifier = &None{}
	}

	err := notifier.Initialize(params)
	if err != nil {

		return nil, err
	}

	notifierMap[key] = notifier

	return notifier, nil
}

func getNotifier() (Notifier, error) {
	data := model.GetVs(confKeys)
	return NewNotifier(data[model.NotifierChannel], data[model.NotifierParams])
}

func Success(order model.Order) {
	notifier, err := getNotifier()
	if err != nil {
		return
	}
	go notifier.Success(order)
}

func NotifyFail(order model.Order, reason string) {
	notifier, err := getNotifier()
	if err != nil {
		return
	}
	go notifier.NotifyFail(order, reason)
}

func NonOrderTransfer(trans model.TronTransfer, wa model.Wallet) {
	notifier, err := getNotifier()
	if err != nil {
		return
	}
	go notifier.NonOrderTransfer(trans, wa)
}

func TronResourceChange(res model.TronResource) {
	notifier, err := getNotifier()
	if err != nil {
		return
	}
	go notifier.TronResourceChange(res)
}

func Welcome() {
	notifier, err := getNotifier()
	if err != nil {
		return
	}

	go notifier.Welcome()
}

// Alert 影响收款的告警（扫块停滞、区块放弃、回调失败、凭证失效等）；调用方负责限频。
// 默认（notifier_alerts=off）只记日志不推送，需要时在后台改为 important / all。
func Alert(title, text string) {
	dispatchAlert(true, title, text)
}

// Notice 运维提示（版本升级、区间跳过、队列拥堵、对账补认单等）；仅在 notifier_alerts=all 时推送。
func Notice(title, text string) {
	dispatchAlert(false, title, text)
}

func dispatchAlert(important bool, title, text string) {
	if !shouldSend(important, GetC(model.NotifierAlerts)) {
		return
	}

	notifier, err := getNotifier()
	if err != nil {
		return
	}

	go notifier.Alert(title, text)
}

// shouldSend 按告警级别判断是否推送；未配置时按 off 处理
func shouldSend(important bool, level string) bool {
	switch level {
	case AlertsAll:
		return true
	case AlertsImportant:
		return important
	default: // off 或未配置
		return false
	}
}

// GetC 便于测试替换的配置读取
var GetC = model.GetC

func Test() error {
	notifier, err := getNotifier()
	if err != nil {
		return err
	}

	return notifier.Test()
}
