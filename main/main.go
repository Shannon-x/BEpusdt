package main

import (
	"context"
	"os"

	"github.com/joho/godotenv"
	"github.com/urfave/cli/v3"
	"github.com/v03413/bepusdt/app/cmd"
	"github.com/v03413/bepusdt/app/conf"
)

// PR 开发/贡献说明
//
// 个人精力有限，人脑也干不过 AI，为了保持项目的可维护性。
// 提交 PR 时，请确保一个 PR 只做一件事，并说明以下内容：
//
//   - 这个 PR 做了什么
//   - 为什么要这么做
//   - Review 重点在哪
//   - 自测 / 实测情况

func init() {
	// 不推荐引导小白参与修改各种配置文件
	_ = godotenv.Load()
}

func main() {
	c := &cli.Command{
		Name:  "BEpusdt",
		Usage: conf.Desc,
		Commands: []*cli.Command{
			cmd.Start,
			cmd.Version,
			cmd.Reset,
			cmd.Scan,
			cmd.Doctor,
			cmd.DB,
		},
	}
	if err := c.Run(context.Background(), os.Args); err != nil {
		panic(err)
	}
}
