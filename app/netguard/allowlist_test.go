package netguard

import (
	"net"
	"net/http/httptest"
	"testing"
)

func TestParseAllowList(t *testing.T) {
	if ParseAllowList("") != nil || ParseAllowList(" , \n ") != nil {
		t.Fatal("empty config must mean no restriction (nil)")
	}

	l := ParseAllowList("1.2.3.4, 10.0.0.0/8\n2001:4860::/32；bad-entry ，5.6.7.8")
	if len(l.Invalid) != 1 || l.Invalid[0] != "bad-entry" {
		t.Fatalf("invalid entries must be reported, got %v", l.Invalid)
	}

	allowed := []string{"1.2.3.4", "10.20.30.40", "2001:4860::8888", "5.6.7.8", "::ffff:1.2.3.4"}
	for _, ip := range allowed {
		if !l.Allows(net.ParseIP(ip)) {
			t.Errorf("%s should be allowed", ip)
		}
	}
	denied := []string{"1.2.3.5", "11.0.0.1", "2001:4861::1"}
	for _, ip := range denied {
		if l.Allows(net.ParseIP(ip)) {
			t.Errorf("%s should be denied", ip)
		}
	}
	if l.Allows(nil) {
		t.Error("unknown client IP must be denied when a whitelist is configured")
	}

	var none *AllowList
	if !none.Allows(net.ParseIP("8.8.8.8")) {
		t.Error("nil whitelist must allow everything")
	}

	onlyInvalid := ParseAllowList("not-an-ip")
	if onlyInvalid.Allows(net.ParseIP("8.8.8.8")) {
		t.Error("a whitelist with only invalid entries must fail closed")
	}
}

func TestCachedAllowListReparsesOnlyOnChange(t *testing.T) {
	var c CachedAllowList
	a, changed := c.Get("1.1.1.1")
	if !changed || a == nil {
		t.Fatal("first Get must parse")
	}
	b, changed := c.Get("1.1.1.1")
	if changed || a != b {
		t.Fatal("same config must reuse cached list")
	}
	if _, changed = c.Get("2.2.2.2"); !changed {
		t.Fatal("changed config must reparse")
	}
}

func TestClientIP(t *testing.T) {
	cases := []struct {
		name   string
		remote string
		xff    string
		xreal  string
		want   string
	}{
		{"direct public peer ignores spoofed headers", "203.0.113.9:5555", "1.2.3.4", "1.2.3.4", "203.0.113.9"},
		{"behind local nginx uses XFF", "127.0.0.1:40000", "8.8.8.8", "", "8.8.8.8"},
		{"spoofed left-most XFF is ignored", "127.0.0.1:40000", "1.2.3.4, 8.8.4.4", "", "8.8.4.4"},
		{"skips trusted proxy hops from the right", "10.0.0.2:1", "9.9.9.9, 10.0.0.3", "", "9.9.9.9"},
		{"falls back to X-Real-IP", "127.0.0.1:1", "", "8.8.8.8", "8.8.8.8"},
		{"LAN merchant without headers", "192.168.1.20:1234", "", "", "192.168.1.20"},
		{"all-private XFF falls back to X-Real-IP", "127.0.0.1:1", "192.168.1.5", "192.168.1.5", "192.168.1.5"},
	}
	for _, c := range cases {
		r := httptest.NewRequest("POST", "/api/v1/order/create-transaction", nil)
		r.RemoteAddr = c.remote
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if c.xreal != "" {
			r.Header.Set("X-Real-IP", c.xreal)
		}
		if got := ClientIP(r); got.String() != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}
