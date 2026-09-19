package task

import (
	"time"

	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
)

func bscInit() {
	registerEvm(
		newEvm(conf.Bsc, block{ConfirmedOffset: 15}, evmNative{Parse: true, Decimal: conf.BscBnbDecimals, TradeType: model.BscBnb}, 0),
		time.Second*5,  // 头部同步间隔
		time.Second*15, // 订单回溯间隔
	)
}
