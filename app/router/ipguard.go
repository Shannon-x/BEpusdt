package router

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/netguard"
)

var apiAllowList netguard.CachedAllowList

// apiIPWhitelist 商户服务端 API 来源 IP 白名单（参考 epusdt api_keys.ip_whitelist）。
//
// 只用于服务端对服务端的下单 / 取消接口；易支付 submit.php 是买家浏览器表单跳转发起的，
// 来源 IP 是买家而不是商户服务器，因此不能也不会套用白名单。
func apiIPWhitelist() gin.HandlerFunc {
	return ipWhitelist(&apiAllowList, func() string { return model.GetC(model.ApiIpWhitelist) })
}

// ipWhitelist 每次请求读取最新配置（后台修改后无需重启），配置未变时复用解析结果
func ipWhitelist(cache *netguard.CachedAllowList, raw func() string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		list, changed := cache.Get(strings.TrimSpace(raw()))
		if changed && list != nil && len(list.Invalid) > 0 {
			log.Warn("API IP 白名单存在无法识别的条目，已忽略：", strings.Join(list.Invalid, ", "))
		}

		ip := netguard.ClientIP(ctx.Request)
		if list.Allows(ip) {
			ctx.Next()

			return
		}

		log.Warn("API 请求来源 IP 不在白名单内，已拒绝：", ip.String(), " ", ctx.Request.URL.Path)
		ctx.AbortWithStatusJSON(200, gin.H{"status_code": 403, "message": "来源 IP 不在白名单内：" + ip.String()})
	}
}
