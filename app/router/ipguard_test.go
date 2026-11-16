package router

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	applog "github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/netguard"
)

func TestIPWhitelistMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	if err := applog.Init(filepath.Join(t.TempDir(), "logs")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(applog.Close)

	config := ""
	var cache netguard.CachedAllowList
	e := gin.New()
	e.POST("/api/v1/order/create-transaction", ipWhitelist(&cache, func() string { return config }), func(c *gin.Context) {
		c.String(http.StatusOK, "passed")
	})

	do := func(remote, xff string) string {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/order/create-transaction", nil)
		r.RemoteAddr = remote
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)

		return w.Body.String()
	}

	if got := do("203.0.113.1:1000", ""); got != "passed" {
		t.Fatalf("empty whitelist must allow all, got %s", got)
	}

	config = "198.51.100.7, 10.0.0.0/8"
	if got := do("198.51.100.7:1000", ""); got != "passed" {
		t.Fatalf("whitelisted IP must pass, got %s", got)
	}
	if got := do("203.0.113.1:1000", "198.51.100.7"); got == "passed" {
		t.Fatal("direct public client must not bypass the whitelist with a forged X-Forwarded-For")
	}
	if got := do("127.0.0.1:5000", "198.51.100.7"); got != "passed" {
		t.Fatalf("request forwarded by local reverse proxy must use XFF, got %s", got)
	}
	if got := do("10.1.2.3:5000", ""); got != "passed" {
		t.Fatalf("CIDR entry must match, got %s", got)
	}

	config = "198.51.100.8" // 后台修改后即时生效
	if got := do("198.51.100.7:1000", ""); got == "passed" {
		t.Fatal("updated whitelist must take effect without restart")
	}
}
