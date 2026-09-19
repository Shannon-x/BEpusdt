package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/urfave/cli/v3"
	"github.com/v03413/bepusdt/app/model"
)

// Scan 扫描运维命令：直接操作数据库中的游标与任务表，运行中的 start 进程每分钟会接手 pending 任务。
var Scan = &cli.Command{
	Name:  "scan",
	Usage: "区块扫描运维：查看游标/任务，登记补扫任务",
	Commands: []*cli.Command{
		{
			Name:   "status",
			Usage:  "查看各链持久化游标、任务统计与回调 outbox 状态",
			Flags:  []cli.Flag{SQLiteFlag, MySQLDSNFlag, PostgresDSNFlag},
			Before: scanBefore,
			After:  scanAfter,
			Action: func(ctx context.Context, cmd *cli.Command) error {
				fmt.Println("扫描游标（已连续完成的高度）：")
				for _, c := range model.AllScanCursors() {
					fmt.Printf("  %-10s %-14d 更新于 %s\n", c.Network, c.Height, c.UpdatedAt.Format(time.DateTime))
				}

				fmt.Println("扫描任务：")
				for status, n := range model.ScanJobCounts("") {
					fmt.Printf("  %-10s %d\n", status, n)
				}

				pending, oldest, dead := model.NotifyOutboxStats()
				fmt.Printf("回调 outbox：待发送 %d，重试耗尽 %d", pending, dead)
				if oldest != nil {
					fmt.Printf("，最早待发送登记于 %s", oldest.Format(time.DateTime))
				}
				fmt.Println()

				return nil
			},
		},
		{
			Name:  "replay",
			Usage: "登记补扫任务：bepusdt scan replay --network solana --from 448096702 --to 448096702",
			Flags: []cli.Flag{
				SQLiteFlag, MySQLDSNFlag, PostgresDSNFlag,
				&cli.StringFlag{Name: "network", Usage: "网络标识：tron / solana / polygon / bsc / ethereum / arbitrum / base / xlayer / plasma / aptos / ton", Required: true},
				&cli.IntFlag{Name: "from", Usage: "起始区块（Solana 为 slot，Aptos 为 version）", Required: true},
				&cli.IntFlag{Name: "to", Usage: "结束区块（含），默认等于 from"},
			},
			Before: scanBefore,
			After:  scanAfter,
			Action: func(ctx context.Context, cmd *cli.Command) error {
				network := cmd.String("network")
				from := int64(cmd.Int("from"))
				to := int64(cmd.Int("to"))
				if to == 0 {
					to = from
				}
				if from <= 0 || to < from {
					return fmt.Errorf("区块范围无效：from=%d to=%d", from, to)
				}
				if _, ok := model.GetAllNetwork()[network]; !ok {
					return fmt.Errorf("未知网络：%s", network)
				}

				job, err := model.CreateScanJob(network, from, to, model.ScanJobKindReplay, model.ScanJobStatusPending, "cli replay", time.Now())
				if err != nil {
					return err
				}

				fmt.Printf("补扫任务已登记 #%d：%s %d → %d\n", job.ID, network, from, to)
				fmt.Println("运行中的 bepusdt start 进程会在 1 分钟内接手执行；补扫是幂等的，不会产生重复回调。")

				return nil
			},
		},
		{
			Name:  "jobs",
			Usage: "查看最近的扫描任务",
			Flags: []cli.Flag{
				SQLiteFlag, MySQLDSNFlag, PostgresDSNFlag,
				&cli.StringFlag{Name: "network", Usage: "只看某个网络"},
				&cli.IntFlag{Name: "limit", Usage: "条数", Value: 30},
			},
			Before: scanBefore,
			After:  scanAfter,
			Action: func(ctx context.Context, cmd *cli.Command) error {
				jobs := model.RecentScanJobs(cmd.String("network"), cmd.Int("limit"))
				fmt.Printf("%-6s %-10s %-10s %-9s %-14s %-14s %-4s %-20s %s\n", "ID", "网络", "类型", "状态", "起始", "结束", "重试", "下次重试", "最后错误")
				for _, j := range jobs {
					fmt.Printf("%-6d %-10s %-10s %-9s %-14d %-14d %-4d %-20s %.60s\n",
						j.ID, j.Network, j.Kind, j.Status, j.FromHeight, j.ToHeight, j.Attempts, j.NextRetryAt.Format(time.DateTime), j.LastError)
				}

				return nil
			},
		},
	},
}

func scanBefore(ctx context.Context, c *cli.Command) (context.Context, error) {
	if err := model.Init(c.String("sqlite"), c.String("mysql"), c.String("postgres")); err != nil {
		return ctx, fmt.Errorf("数据库初始化失败 %w", err)
	}

	return ctx, nil
}

func scanAfter(ctx context.Context, c *cli.Command) error {
	model.Close()

	return nil
}
