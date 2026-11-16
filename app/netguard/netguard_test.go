package netguard

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIsPublicIP(t *testing.T) {
	cases := map[string]bool{
		"8.8.8.8":              true,
		"1.1.1.1":              true,
		"2606:4700:4700::1111": true,
		"64:ff9b::808:808":     true, // NAT64 内嵌 8.8.8.8

		"127.0.0.1":          false,
		"10.1.2.3":           false,
		"172.16.0.1":         false,
		"192.168.1.1":        false,
		"169.254.169.254":    false, // 云元数据
		"100.64.0.1":         false, // CGNAT
		"0.0.0.0":            false,
		"255.255.255.255":    false,
		"224.0.0.1":          false,
		"::1":                false,
		"fc00::1":            false,
		"fe80::1":            false,
		"::ffff:127.0.0.1":   false, // IPv4 映射
		"::ffff:10.0.0.1":    false,
		"64:ff9b::7f00:1":    false, // NAT64 内嵌 127.0.0.1
		"64:ff9b::a9fe:a9fe": false, // NAT64 内嵌 169.254.169.254
		"2001:db8::1":        false,
	}
	for raw, want := range cases {
		if got := IsPublicIP(net.ParseIP(raw)); got != want {
			t.Errorf("IsPublicIP(%s) = %v, want %v", raw, got, want)
		}
	}
	if IsPublicIP(nil) {
		t.Error("nil IP must not be public")
	}
}

func stubLookup(t *testing.T, fn func(host string) ([]net.IPAddr, error)) {
	t.Helper()
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, host string) ([]net.IPAddr, error) { return fn(host) }
	t.Cleanup(func() { lookupIPAddr = old })
}

func TestCheckURL(t *testing.T) {
	stubLookup(t, func(host string) ([]net.IPAddr, error) {
		switch host {
		case "merchant.example":
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		case "rebind.example": // 一个公网 + 一个内网，任一内网即拒绝
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("10.0.0.8")}}, nil
		case "meta.example":
			return []net.IPAddr{{IP: net.ParseIP("169.254.169.254")}}, nil
		}

		return nil, errors.New("no such host")
	})

	ok := []string{
		"https://merchant.example/notify",
		"http://8.8.8.8:8080/cb",
		"https://flaky-dns.example/notify", // DNS 失败放行，由拨号时兜底
	}
	for _, raw := range ok {
		if err := CheckURL(raw); err != nil {
			t.Errorf("CheckURL(%q) unexpected error: %v", raw, err)
		}
	}

	blocked := []string{
		"http://127.0.0.1/notify",
		"http://localhost:8080/notify",
		"http://api.localhost/notify",
		"http://LOCALHOST./notify",
		"http://[::1]/notify",
		"http://169.254.169.254/latest/meta-data",
		"http://192.168.1.10/notify",
		"http://[fe80::1%25eth0]/notify",
		"https://rebind.example/notify",
		"https://meta.example/notify",
	}
	for _, raw := range blocked {
		err := CheckURL(raw)
		if !IsBlocked(err) {
			t.Errorf("CheckURL(%q) must be blocked, got %v", raw, err)
		}
	}

	invalid := []string{"", "ftp://merchant.example/x", "javascript:alert(1)", "http:///nohost"}
	for _, raw := range invalid {
		if err := CheckURL(raw); err == nil {
			t.Errorf("CheckURL(%q) must fail", raw)
		}
	}
}

func TestDialControlBlocksResolvedPrivateAddress(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:80", "[::1]:443", "169.254.169.254:80", "10.0.0.1:8080"} {
		if err := dialControl("tcp", addr, nil); !IsBlocked(err) {
			t.Errorf("dialControl(%s) must block, got %v", addr, err)
		}
	}
	if err := dialControl("tcp", "93.184.216.34:443", nil); err != nil {
		t.Errorf("public address must pass: %v", err)
	}
}

func TestGuardedClientRefusesLoopbackAndPrivateClientAllowsIt(t *testing.T) {
	hit := make(chan struct{}, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit <- struct{}{}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	_, err := NewClient(2*time.Second, false).Get(srv.URL)
	if !IsBlocked(err) {
		t.Fatalf("guarded client must refuse loopback target, got %v", err)
	}
	select {
	case <-hit:
		t.Fatal("blocked request must never reach the server")
	default:
	}

	resp, err := NewClient(2*time.Second, true).Get(srv.URL)
	if err != nil {
		t.Fatalf("private client should reach local server: %v", err)
	}
	_ = resp.Body.Close()
}

func TestCheckRedirectPolicy(t *testing.T) {
	// 跳转后的新连接同样经过 dialControl（见上一个测试），这里只校验跳转策略本身
	redirect := NewClient(time.Second, false).CheckRedirect
	from, _ := http.NewRequest(http.MethodGet, "https://merchant.example/notify", nil)

	next, _ := http.NewRequest(http.MethodGet, "https://merchant.example/v2/notify", nil)
	if err := redirect(next, []*http.Request{from}); err != nil {
		t.Fatalf("plain https redirect should be allowed: %v", err)
	}

	bad, _ := http.NewRequest(http.MethodGet, "file:///etc/passwd", nil)
	if err := redirect(bad, []*http.Request{from}); err == nil {
		t.Fatal("redirect to non-http scheme must be refused")
	}

	via := make([]*http.Request, 5)
	if err := redirect(next, via); err == nil {
		t.Fatal("too many redirects must be refused")
	}
}
