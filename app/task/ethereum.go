package task

import (
	"time"

	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
)

func ethInit() {
	registerEvm(
		newEvm(conf.Ethereum, block{ConfirmedOffset: 12}, evmNative{Parse: true, Decimal: conf.EthereumEthDecimals, TradeType: model.EthereumEth}, time.Second*2),
		time.Second*12, // 头部同步间隔
		time.Second*20, // 订单回溯间隔
	)
}
