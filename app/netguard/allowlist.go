package netguard

import (
	"net"
	"net/http"
	"strings"
	"sync"
)

// AllowList 来源 IP 白名单，条目可以是单个 IP 或 CIDR 网段
type AllowList struct {
	nets    []*net.IPNet
	Invalid []string // 无法解析的条目，调用方可记录日志
}

// ParseAllowList 解析白名单配置；条目用逗号、分号（中英文均可）、空白或换行分隔。空配置返回 nil（表示不限制）
func ParseAllowList(raw string) *AllowList {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '，' || r == ';' || r == '；' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
	if len(fields) == 0 {
		return nil
	}

	list := &AllowList{}
	for _, f := range fields {
		if strings.Contains(f, "/") {
			if _, n, err := net.ParseCIDR(f); err == nil {
				list.nets = append(list.nets, n)
				continue
			}
		} else if ip := net.ParseIP(f); ip != nil {
			bits := net.IPv6len * 8
			if v4 := ip.To4(); v4 != nil {
				ip, bits = v4, net.IPv4len*8
			}
			list.nets = append(list.nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		list.Invalid = append(list.Invalid, f)
	}

	return list
}

// Allows 判断 IP 是否在白名单内；nil 白名单表示不限制。
// 注意：如果配置了白名单但所有条目都无效，则拒绝所有请求（宁可误拦也不放行）。
func (l *AllowList) Allows(ip net.IP) bool {
	if l == nil {
		return true
	}
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	for _, n := range l.nets {
		if n.Contains(ip) {
			return true
		}
	}

	return false
}

// CachedAllowList 按原始配置字符串缓存解析结果，配置不变时不重复解析
type CachedAllowList struct {
	mu   sync.Mutex
	raw  string
	list *AllowList
	init bool
}

// Get 返回 raw 对应的白名单，以及 raw 相比上次是否发生变化（便于只在变化时打印无效条目日志）
func (c *CachedAllowList) Get(raw string) (*AllowList, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.init && c.raw == raw {
		return c.list, false
	}

	c.raw, c.list, c.init = raw, ParseAllowList(raw), true

	return c.list, true
}

// isTrustedProxy 直连对端是本机或内网地址时，认为是反向代理（Nginx / Caddy / 1Panel / 宝塔 等）
func isTrustedProxy(ip net.IP) bool {
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

// ClientIP 识别调用方真实 IP，用于白名单判断。
//
// 与 gin 默认的 ClientIP() 不同，这里不会无条件信任 X-Forwarded-For：
// 只有当直连对端是本机 / 内网地址（即请求来自反向代理）时才读取转发头，
// 否则直接使用 TCP 对端地址，防止公网直连时伪造请求头绕过白名单。
func ClientIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		host = strings.TrimSpace(r.RemoteAddr)
	}

	remote := net.ParseIP(host)
	if !isTrustedProxy(remote) {
		return remote
	}

	// X-Forwarded-For 从右往左取第一个非代理地址：最右侧的条目由最近一层可信代理写入，无法被客户端伪造
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		for i := len(parts) - 1; i >= 0; i-- {
			ip := net.ParseIP(strings.TrimSpace(parts[i]))
			if ip == nil {
				break
			}
			if !isTrustedProxy(ip) {
				return ip
			}
		}
	}

	if ip := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); ip != nil {
		return ip
	}

	return remote
}
