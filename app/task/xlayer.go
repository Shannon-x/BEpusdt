package task

import (
	"time"

	"github.com/v03413/bepusdt/app/conf"
)

func xlayerInit() {
	registerEvm(
		newEvm(conf.Xlayer, block{RollDelayOffset: 3, ConfirmedOffset: 12}, evmNative{}, time.Millisecond*300),
		time.Second*3,  // 头部同步间隔
		time.Second*15, // 订单回溯间隔
	)
}
