package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/v03413/bepusdt/app/exchange"
	"github.com/v03413/bepusdt/app/handler/base"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

type Wallet struct {
}

// 交易所类型钱包的只读 API 凭证；地址填交易所账户 UID
type exchangeCredReq struct {
	ApiKey     string `json:"api_key"`
	ApiSecret  string `json:"api_secret"`
	Passphrase string `json:"passphrase"`
}

type wAddReq struct {
	Name        string `json:"name"`
	Remark      string `json:"remark"`
	Address     string `json:"address" binding:"required"`
	TradeType   string `json:"trade_type" binding:"required"`
	OtherNotify uint8  `json:"other_notify"`
	exchangeCredReq
}

type wModReq struct {
	base.IDRequest
	Name        *string `json:"name"`
	Status      *uint8  `json:"status"`
	Address     *string `json:"address"`
	Remark      *string `json:"remark"`
	TradeType   *string `json:"trade_type"`
	OtherNotify *uint8  `json:"other_notify"`
	exchangeCredReq
}

type wVerifyReq struct {
	ID        int    `json:"id"` // 已有钱包 ID，凭证字段留空时使用其已保存的凭证
	TradeType string `json:"trade_type" binding:"required"`
	Address   string `json:"address" binding:"required"`
	exchangeCredReq
}

// mergeCredential 把请求中的凭证合并到已有凭证上（留空表示不修改）
func mergeCredential(existing model.ExchangeCredential, req exchangeCredReq) model.ExchangeCredential {
	if v := strings.TrimSpace(req.ApiKey); v != "" {
		existing.ApiKey = v
	}
	if v := strings.TrimSpace(req.ApiSecret); v != "" {
		existing.ApiSecret = v
	}
	if v := strings.TrimSpace(req.Passphrase); v != "" {
		existing.Passphrase = v
	}

	return existing
}

// verifyExchangeCredential 用只读接口校验凭证可用，并确认凭证所属账户 UID 与填写的地址一致
func verifyExchangeCredential(tradeType model.TradeType, address string, cred model.ExchangeCredential) (string, error) {
	network := model.TradeNetwork(tradeType)
	if cred.ApiKey == "" || cred.ApiSecret == "" {
		return "", errors.New("交易所钱包需要填写只读 API Key 与 Secret")
	}
	if network == model.Network("okx") && cred.Passphrase == "" {
		return "", errors.New("OKX 需要填写创建 API Key 时设置的 Passphrase")
	}

	cli, err := exchange.New(string(network), model.Endpoints(network), utils.NewHttpClient())
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	uid, err := cli.VerifyUID(ctx, exchange.Credential{ApiKey: cred.ApiKey, ApiSecret: cred.ApiSecret, Passphrase: cred.Passphrase})
	if err != nil {
		return "", fmt.Errorf("交易所 API 校验失败：%v", err)
	}
	if uid != strings.TrimSpace(address) {
		return uid, fmt.Errorf("API 凭证所属账户 UID 为 %s，与填写的 %s 不一致", uid, address)
	}

	return uid, nil
}

type wListReq struct {
	base.ListRequest
	Name    string `json:"name"`
	Address string `json:"address"`
	Trade   string `json:"trade_type"`
}

func (Wallet) Add(ctx *gin.Context) {
	var req wAddReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	if !model.IsSupportedTradeType(model.TradeType(req.TradeType)) {
		base.BadRequest(ctx, fmt.Sprintf("不支持的交易类型: %s", req.TradeType))

		return
	}

	var wallet = model.Wallet{
		Name:        strings.TrimSpace(req.Name),
		Remark:      req.Remark,
		Address:     strings.TrimSpace(req.Address),
		MatchAddr:   strings.TrimSpace(req.Address),
		TradeType:   req.TradeType,
		Status:      model.WaStatusEnable,
		OtherNotify: req.OtherNotify,
	}

	if err := wallet.Validate(); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	if model.IsExchange(model.TradeType(wallet.TradeType)) {
		cred := mergeCredential(model.ExchangeCredential{}, req.exchangeCredReq)
		if _, err := verifyExchangeCredential(model.TradeType(wallet.TradeType), wallet.Address, cred); err != nil {
			base.BadRequest(ctx, err.Error())

			return
		}
		if err := wallet.SetCredentials(cred); err != nil {
			base.Error(ctx, err)

			return
		}
	}

	if err := model.Db.Create(&wallet).Error; err != nil {
		base.Error(ctx, err)

		return
	}

	base.Response(ctx, 200, "success")
}

// ExchangeVerify 校验交易所只读 API 凭证是否可用、是否属于填写的 UID
func (Wallet) ExchangeVerify(ctx *gin.Context) {
	var req wVerifyReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	tradeType := model.TradeType(req.TradeType)
	if !model.IsExchange(tradeType) {
		base.BadRequest(ctx, "该交易类型不是交易所内部转账")

		return
	}

	var existing model.ExchangeCredential
	if req.ID > 0 {
		var w model.Wallet
		model.Db.Where("id = ?", req.ID).Find(&w)
		if w.ID != 0 {
			existing, _ = w.GetCredentials()
		}
	}

	uid, err := verifyExchangeCredential(tradeType, req.Address, mergeCredential(existing, req.exchangeCredReq))
	if err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	base.Ok(ctx, gin.H{"uid": uid, "match": true})
}

func (Wallet) List(ctx *gin.Context) {
	var req wListReq
	if err := ctx.ShouldBind(&req); err != nil {
		base.Response(ctx, 400, err.Error())

		return
	}

	var data []model.Wallet
	var db = model.Db

	if req.Name != "" {
		db = db.Where("name LIKE ?", "%"+req.Name+"%")
	}
	if req.Address != "" {
		db = db.Where("address LIKE ?", "%"+req.Address+"%")
	}
	if req.Trade != "" {
		db = db.Where("trade_type LIKE ?", "%"+req.Trade+"%")
	}

	var total int64

	db.Model(&model.Wallet{}).Count(&total)

	err := db.Limit(req.Size).Offset((req.Page - 1) * req.Size).Order("id " + req.Sort).Find(&data).Error
	if err != nil {
		base.Response(ctx, 400, err.Error())

		return
	}

	base.Response(ctx, 200, data, total)
}

func (Wallet) Mod(ctx *gin.Context) {
	var req wModReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	var w model.Wallet
	model.Db.Where("id = ?", req.ID).Find(&w)
	if w.ID == 0 {
		base.BadRequest(ctx, "钱包不存在")

		return
	}

	if req.Name != nil {
		w.Name = strings.TrimSpace(*req.Name)
	}
	if req.Remark != nil {
		w.Remark = *req.Remark
	}
	if req.Address != nil {
		w.Address = strings.TrimSpace(*req.Address)
		w.MatchAddr = strings.TrimSpace(*req.Address)
	}
	if req.TradeType != nil {
		if !model.IsSupportedTradeType(model.TradeType(*req.TradeType)) {
			base.BadRequest(ctx, fmt.Sprintf("不支持的交易类型: %s", *req.TradeType))

			return
		}

		w.TradeType = *req.TradeType
	}
	if req.Status != nil {
		w.Status = *req.Status
	}
	if req.OtherNotify != nil {
		w.OtherNotify = *req.OtherNotify
	}

	if err := w.Validate(); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	if model.IsExchange(model.TradeType(w.TradeType)) {
		existing, _ := w.GetCredentials()
		cred := mergeCredential(existing, req.exchangeCredReq)
		changed := req.ApiKey != "" || req.ApiSecret != "" || req.Passphrase != "" || req.Address != nil
		if changed || !w.HasCredentials {
			if _, err := verifyExchangeCredential(model.TradeType(w.TradeType), w.Address, cred); err != nil {
				base.BadRequest(ctx, err.Error())

				return
			}
			if err := w.SetCredentials(cred); err != nil {
				base.Error(ctx, err)

				return
			}
		}
	} else if w.Credentials != "" { // 从交易所类型改为链上类型，清除凭证
		w.Credentials = ""
	}

	if err := model.Db.Save(&w).Error; err != nil {
		base.Error(ctx, err)

		return
	}

	base.Response(ctx, 200, "修改成功")
}

func (Wallet) Del(ctx *gin.Context) {
	var req base.IDRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		base.BadRequest(ctx, err.Error())

		return
	}

	model.Db.Where("id = ?", req.ID).Delete(&model.Wallet{})

	base.Response(ctx, 200, "删除成功")
}
