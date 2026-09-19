package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/urfave/cli/v3"
	"github.com/v03413/bepusdt/app"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/model/migration"
)

// Doctor 升级 / 部署自检：数据库、结构迁移、配置、钱包与节点、回调积压、扫描任务
var Doctor = &cli.Command{
	Name:   "doctor",
	Usage:  "自检：数据库连接与结构、配置项、钱包与 RPC 节点、回调与扫描任务状态（升级后建议运行一次）",
	Flags:  []cli.Flag{SQLiteFlag, MySQLDSNFlag, PostgresDSNFlag},
	Before: scanBefore,
	After:  scanAfter,
	Action: doctor,
}

type doctorReport struct {
	problems int
}

func (r *doctorReport) ok(format string, a ...any) {
	fmt.Printf("  ✅ "+format+"\n", a...)
}

func (r *doctorReport) warn(format string, a ...any) {
	fmt.Printf("  ⚠️  "+format+"\n", a...)
}

func (r *doctorReport) fail(format string, a ...any) {
	r.problems++
	fmt.Printf("  ❌ "+format+"\n", a...)
}

func doctor(ctx context.Context, c *cli.Command) error {
	r := &doctorReport{}
	fmt.Printf("BEpusdt %s 自检（%s）\n", app.Version, time.Now().Format(time.DateTime))

	fmt.Println("\n[数据库]")
	r.ok("类型：%s", model.Driver())
	for _, m := range model.Models() {
		if !model.Db.Migrator().HasTable(m) {
			r.fail("缺少数据表 %T", m)
		}
	}
	applied := migration.Applied(model.Db)
	r.ok("已执行迁移 %d 条：%v", len(applied), applied)
	if missing := model.MissingDefaultConf(); len(missing) > 0 {
		r.warn("缺少默认配置项（启动时会自动补齐）：%v", missing)
	} else {
		r.ok("默认配置项齐全")
	}
	if prev := model.GetK(model.SystemVersion); prev != "" && prev != app.Version {
		r.warn("数据库记录的上次运行版本为 %s，当前 %s：首次启动会自动迁移并发送升级提示", prev, app.Version)
	}

	fmt.Println("\n[钱包与节点]")
	var wallets []model.Wallet
	model.Db.Where("status = ?", model.WaStatusEnable).Find(&wallets)
	if len(wallets) == 0 {
		r.warn("没有启用中的钱包，无法收款")
	}
	byNetwork := map[model.Network]int{}
	for _, w := range wallets {
		tt := model.TradeType(w.TradeType)
		if !model.IsSupportedTradeType(tt) {
			r.fail("钱包 #%d 的交易类型 %s 已不再支持", w.ID, w.TradeType)

			continue
		}
		byNetwork[model.TradeNetwork(tt)]++
		if model.IsExchange(tt) && !w.HasCredentials {
			r.fail("交易所钱包 #%d（%s，UID %s）未配置 API 凭证，入账无法识别", w.ID, w.TradeType, w.Address)
		}
	}
	networks := make([]string, 0, len(byNetwork))
	for n := range byNetwork {
		networks = append(networks, string(n))
	}
	sort.Strings(networks)
	for _, n := range networks {
		endpoints := model.Endpoints(model.Network(n))
		switch {
		case len(endpoints) == 0:
			r.fail("%s：%d 个钱包，但未配置 RPC / API 节点", n, byNetwork[model.Network(n)])
		case len(endpoints) == 1 && !model.IsExchangeNetwork(model.Network(n)):
			r.warn("%s：%d 个钱包，只有 1 个节点 %s，建议再配置 1 个不同服务商的备用节点", n, byNetwork[model.Network(n)], endpoints[0])
		default:
			r.ok("%s：%d 个钱包，%d 个节点", n, byNetwork[model.Network(n)], len(endpoints))
		}
	}
	if byNetwork[model.Network("tron")] > 0 && len(model.GetTronGridApiKeys()) == 0 {
		r.warn("Tron 未配置 TronGrid API Key，公共节点限流时会漏块")
	}

	fmt.Println("\n[回调与扫描]")
	pending, oldest, dead := model.NotifyOutboxStats()
	if dead > 0 {
		r.warn("有 %d 个订单回调重试耗尽（dead），请在后台手动重发", dead)
	}
	if pending > 0 && oldest != nil && time.Since(*oldest) > 30*time.Minute {
		r.warn("有 %d 个回调待发送，最早等待 %s，请检查商户回调地址", pending, time.Since(*oldest).Round(time.Minute))
	} else {
		r.ok("回调 outbox：待发送 %d，dead %d", pending, dead)
	}
	jobs := model.ScanJobCounts("")
	if jobs[model.ScanJobStatusFailed] > 0 {
		r.warn("有 %d 个扫描任务自动重试耗尽（failed），可用 bepusdt scan replay 补扫", jobs[model.ScanJobStatusFailed])
	} else {
		r.ok("扫描任务：pending %d，running %d，deferred %d", jobs[model.ScanJobStatusPending], jobs[model.ScanJobStatusRunning], jobs[model.ScanJobStatusDeferred])
	}
	for _, cur := range model.AllScanCursors() {
		r.ok("游标 %-8s %d（%s）", cur.Network, cur.Height, cur.UpdatedAt.Format(time.DateTime))
	}
	if model.GetC(model.NotifierChannel) == "" || model.GetC(model.NotifierChannel) == "none" {
		r.warn("未配置通知渠道，扫描停滞 / 回调失败等告警将无处发送")
	}

	fmt.Println()
	if r.problems > 0 {
		fmt.Printf("发现 %d 个问题，请处理后再启动。\n", r.problems)
		os.Exit(1)
	}
	fmt.Println("自检通过。")

	return nil
}
