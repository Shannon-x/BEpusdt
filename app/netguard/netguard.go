// Package netguard 出站请求安全防护：阻止商户回调地址被用来访问内网 / 本机 / 云元数据服务（SSRF）。
//
// 参考 GMWalletApp/epusdt 的 util/security/url_guard.go，并在其基础上加了一层「拨号时校验」：
//
//   - CheckURL：创建订单时快速失败，商户能立即看到明确的错误提示；
//   - NewClient：回调真正发起连接时，在 net.Dialer.Control 中校验 DNS 解析后的真实 IP，
//     可以防住 DNS 重绑定（创建时解析为公网、回调时解析为内网）和 302 跳转到内网等绕过方式。
//
// 真正的拦截以拨号时校验为准；CheckURL 在 DNS 暂时解析失败时会放行，避免商户域名 DNS 抖动导致无法下单。
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

const dnsLookupTimeout = 3 * time.Second

// lookupIPAddr 可在测试中替换
var lookupIPAddr = func(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

// BlockedError 目标地址属于内网 / 保留地址
type BlockedError struct {
	Host string
	IP   net.IP
}

func (e *BlockedError) Error() string {
	if e.IP == nil {
		return fmt.Sprintf("地址 %s 不允许访问", e.Host)
	}
	if e.Host == "" || e.Host == e.IP.String() {
		return fmt.Sprintf("地址 %s 属于内网或保留地址，已拦截", e.IP)
	}

	return fmt.Sprintf("地址 %s 解析到内网或保留地址 %s，已拦截", e.Host, e.IP)
}

// IsBlocked 判断错误链中是否包含 BlockedError
func IsBlocked(err error) bool {
	var be *BlockedError

	return errors.As(err, &be)
}

// 标准库 net.IP 判定之外，额外拦截的保留网段
var extraBlocked = mustParseCIDRs(
	"100.64.0.0/10", // 运营商级 NAT（CGNAT），Tailscale 等组网也使用该段
	"192.0.0.0/24",  // IETF 协议分配
	"240.0.0.0/4",   // 保留地址（含 255.255.255.255）
	"2001:db8::/32", // IPv6 文档地址
)

// NAT64 前缀：内嵌的 IPv4 需要单独判断，避免 64:ff9b::7f00:1 这类写法绕过
var nat64 = mustParseCIDRs("64:ff9b::/96")[0]

func mustParseCIDRs(list ...string) []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(list))
	for _, s := range list {
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			panic(err)
		}
		nets = append(nets, n)
	}

	return nets
}

// IsPublicIP 判断 IP 是否为可安全访问的公网单播地址
func IsPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	} else if len(ip) == net.IPv6len && nat64.Contains(ip) {
		return IsPublicIP(net.IP(ip[12:16]))
	}

	if ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || // 169.254.0.0/16 含云厂商元数据地址
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() ||
		!ip.IsGlobalUnicast() {
		return false
	}
	for _, n := range extraBlocked {
		if n.Contains(ip) {
			return false
		}
	}

	return true
}

// CheckURL 校验回调地址：必须是 http(s)，且主机不能是 localhost / 内网 / 保留地址。
// 域名会做一次 DNS 解析，任一解析结果为内网即拒绝；DNS 解析失败时放行，交由拨号时校验兜底。
func CheckURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("地址不能为空")
	}

	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("地址格式错误：%w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return errors.New("地址协议必须是 http 或 https")
	}

	host := u.Hostname()
	if host == "" {
		return errors.New("地址缺少主机名")
	}
	if strings.Contains(host, "%") { // IPv6 zone，如 fe80::1%eth0
		return &BlockedError{Host: host}
	}

	lower := strings.TrimSuffix(strings.ToLower(host), ".")
	if lower == "localhost" || strings.HasSuffix(lower, ".localhost") {
		return &BlockedError{Host: host}
	}

	if ip := net.ParseIP(host); ip != nil {
		if !IsPublicIP(ip) {
			return &BlockedError{Host: host, IP: ip}
		}

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), dnsLookupTimeout)
	defer cancel()

	addrs, err := lookupIPAddr(ctx, host)
	if err != nil {
		return nil // 解析失败不阻断下单，拨号时会再次校验
	}
	for _, addr := range addrs {
		if !IsPublicIP(addr.IP) {
			return &BlockedError{Host: host, IP: addr.IP}
		}
	}

	return nil
}

// dialControl 在 TCP 连接建立前校验真实目标 IP（已完成 DNS 解析）
func dialControl(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return &BlockedError{Host: address}
	}

	ip := net.ParseIP(host)
	if !IsPublicIP(ip) {
		return &BlockedError{Host: host, IP: ip}
	}

	return nil
}

// NewClient 创建用于商户回调的 HTTP 客户端。
//
// allowPrivate=false 时：每一次拨号（包括 302 跳转后的新连接）都会校验目标 IP，只允许公网地址；
// 为了能校验到真实目标，此模式下不使用 HTTP_PROXY 等环境变量代理，回调直连商户。
//
// allowPrivate=true 时：行为与原先一致（允许内网，遵循环境变量代理），用于商户系统与网关部署在同一内网的场景。
func NewClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	if !allowPrivate {
		dialer.Control = dialControl
		transport.Proxy = nil
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("回调重定向次数过多")
			}
			if s := req.URL.Scheme; s != "http" && s != "https" {
				return fmt.Errorf("回调重定向到不支持的协议：%s", s)
			}

			return nil
		},
	}
}
