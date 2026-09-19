package router

import (
	"encoding/json"
	"html/template"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/v03413/bepusdt/static"
)

func TestThemeAssetsAreServedWithRevalidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	registerAssetsFromFS(e, static.Checkout, "checkout/sufe", "/checkout/sufe/assets")

	req := httptest.NewRequest(http.MethodGet, "/checkout/sufe/assets/js/checkout.js", nil)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("theme asset must be served, got %d", w.Code)
	}
	// 资源文件名固定，必须要求浏览器每次校验，否则升级后仍用缓存里的旧脚本
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("theme asset must be revalidated, Cache-Control=%q", cc)
	}
	if !strings.Contains(w.Body.String(), "currentAmountText") {
		t.Fatal("served asset should be the bundled checkout.js")
	}
}

// 收银台文案不得残留未替换的占位符：sufe 主题不通过 i18next 变量插值，
// 一旦脚本与语言文件版本不一致，占位符会原样显示给付款人。
func TestSufeLocalesHaveNoAmountPlaceholder(t *testing.T) {
	entries, err := fs.ReadDir(static.Checkout, "checkout/sufe/assets/locales")
	if err != nil {
		t.Fatalf("read sufe locales: %v", err)
	}
	if len(entries) < 14 {
		t.Fatalf("expected at least 14 locale files, got %d", len(entries))
	}

	for _, entry := range entries {
		name := "checkout/sufe/assets/locales/" + entry.Name()
		raw, err := fs.ReadFile(static.Checkout, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(raw), "{{amount}}") {
			t.Fatalf("%s still contains the {{amount}} placeholder", name)
		}

		var doc struct {
			Payment map[string]any `json:"payment"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s is not valid JSON: %v", name, err)
		}
		for _, key := range []string{"instructionFee", "amountCaption", "paymentAmount"} {
			if v, ok := doc.Payment[key].(string); !ok || strings.TrimSpace(v) == "" {
				t.Fatalf("%s misses translation for %s", name, key)
			}
		}
	}

	view, err := fs.ReadFile(static.Checkout, "checkout/sufe/views/checkout.html")
	if err != nil {
		t.Fatalf("read sufe template: %v", err)
	}
	if strings.Contains(string(view), "{{amount}}") {
		t.Fatal("sufe template still contains the {{amount}} placeholder")
	}
	// 未配置客服链接时按钮默认隐藏，由脚本在拿到订单后决定是否展示
	if !strings.Contains(string(view), `id="supportBtn"`) || !strings.Contains(string(view), "rel=\"noopener\" hidden") {
		t.Fatal("support button must default to hidden")
	}

	// 二维码中心只放一个币种图标：叠加网络小徽标在小圆里会错位重叠
	if strings.Contains(string(view), "qrNetworkLogo") || strings.Contains(string(view), "qr-logo-stack") {
		t.Fatal("QR badge must contain a single token icon")
	}
	badge := string(view)
	start := strings.Index(badge, `id="qrLogoBadge"`)
	if start < 0 {
		t.Fatal("QR badge markup not found")
	}
	end := strings.Index(badge[start:], "</div>")
	if end < 0 || strings.Count(badge[start:start+end], "<img") != 1 {
		t.Fatal("QR badge must hold exactly one image")
	}
}

// 主题资源的文件名固定，升级后 URL 不变会被浏览器复用旧文件；
// 模板必须给每个资源带上版本号，否则升级后页面仍跑旧脚本（曾导致语言切换失效、占位符外露）。
func TestCheckoutTemplatesVersionTheirAssets(t *testing.T) {
	for _, theme := range []string{"sufe", "official", "langge"} {
		name := "checkout/" + theme + "/views/checkout.html"
		raw, err := fs.ReadFile(static.Checkout, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}

		tmpl, err := template.New("checkout").Parse(string(raw))
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		var out strings.Builder
		if err := tmpl.Execute(&out, map[string]any{"trade_id": "T1", "version": "v9.9.9"}); err != nil {
			t.Fatalf("execute %s: %v", name, err)
		}

		rendered := out.String()
		if strings.Contains(rendered, "{{") {
			t.Fatalf("%s still holds unrendered template syntax", name)
		}
		if !strings.Contains(rendered, `window.__CHECKOUT_VERSION__ = "v9.9.9"`) {
			t.Fatalf("%s must expose the asset version to its script", name)
		}

		for _, ext := range []string{".js", ".css"} {
			for _, ref := range assetRefs(rendered, ext) {
				if !strings.Contains(ref, "?v=v9.9.9") {
					t.Fatalf("%s references %s without a version", name, ref)
				}
			}
		}
	}
}

// assetRefs 取出 rendered 中所有指向本主题 assets 目录、以 ext 结尾的引用
func assetRefs(rendered, ext string) []string {
	refs := make([]string, 0)
	for _, part := range strings.Split(rendered, `"`) {
		if strings.HasPrefix(part, "/checkout/") && strings.Contains(part, ext) {
			refs = append(refs, part)
		}
	}

	return refs
}
