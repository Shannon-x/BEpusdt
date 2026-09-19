package admin

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/v03413/bepusdt/app/handler/base"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/task"
)

type Scan struct {
}

type scanReplayReq struct {
	Network string `json:"network" binding:"required"` // 网络标识，如 solana / polygon / tron
	From    int64  `json:"from" binding:"required"`    // 起始区块（Solana 为 slot，Aptos 为 version）
	To      int64  `json:"to"`                         // 结束区块（含），为空则等于 from
}

type scanJobsReq struct {
	Network string `json:"network"`
	Limit   int    `json:"limit"`
}

// Status 总览：各链扫描状态（最新高度、连续游标、最近成功、成功率、队列、回溯、放弃数、任务统计、节点）+ 回调 outbox + 未认单入账
func (Scan) Status(ctx *gin.Context) {
	base.Ok(ctx, task.GetOverview())
}

// Jobs 最近的扫描任务（放弃的区块、跳过的区间、回放）
func (Scan) Jobs(ctx *gin.Context) {
	var req scanJobsReq
	_ = ctx.ShouldBindJSON(&req)
	if req.Limit <= 0 || req.Limit > 200 {
		req.Limit = 50
	}

	base.Ok(ctx, model.RecentScanJobs(strings.ToLower(strings.TrimSpace(req.Network)), req.Limit))
}

// Replay 补扫指定区块区间。幂等：已成功的订单不会被重复处理，非订单通知按交易哈希去重
func (Scan) Replay(ctx *gin.Context) {
	var req scanReplayReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	if req.To == 0 {
		req.To = req.From
	}

	network := strings.ToLower(strings.TrimSpace(req.Network))
	enqueued, err := task.Replay(network, req.From, req.To)
	if err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	base.Ok(ctx, gin.H{"network": network, "from": req.From, "to": req.To, "enqueued": enqueued})
}
