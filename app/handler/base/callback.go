package base

import (
	"errors"
	"fmt"

	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/netguard"
	"github.com/v03413/bepusdt/app/utils"
)

// CheckNotifyURL 校验商户回调地址：格式必须合法，且默认不允许指向 localhost / 内网 / 保留地址（防 SSRF）。
// 回调发出时还会在拨号阶段再次校验真实 IP，这里只负责让商户在下单时就看到明确错误。
func CheckNotifyURL(raw string) error {
	if !utils.IsAllowedCallbackURL(raw) {
		return errors.New("notify_url 地址不合法")
	}
	if model.CallbackAllowPrivate() {
		return nil
	}
	if err := netguard.CheckURL(raw); err != nil {
		return fmt.Errorf("notify_url 地址不合法：%v。如商户系统与 BEpusdt 部署在同一服务器或内网，请在后台「API设置」开启「回调允许内网地址」", err)
	}

	return nil
}
