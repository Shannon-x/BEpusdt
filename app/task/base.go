package task

import (
	"time"

	"github.com/v03413/bepusdt/app/conf"
)

func baseInit() {
	registerEvm(
		newEvm(conf.Base, block{ConfirmedOffset: 40}, evmNative{}, 0),
		time.Second*5,  // 头部同步间隔
		time.Second*15, // 订单回溯间隔
	)
}
