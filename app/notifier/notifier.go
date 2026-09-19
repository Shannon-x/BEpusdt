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

// Alert 发送系统告警；调用方负责限频
func Alert(title, text string) {
	notifier, err := getNotifier()
	if err != nil {
		return
	}

	go notifier.Alert(title, text)
}

func Test() error {
	notifier, err := getNotifier()
	if err != nil {
		return err
	}

	return notifier.Test()
}
