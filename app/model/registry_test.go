package model

import (
	"reflect"
	"testing"
	"unicode/utf8"
)

func TestSplitEndpoints(t *testing.T) {
	got := SplitEndpoints(" https://a.example/rpc, https://b.example/rpc\nhttps://a.example/rpc  https://c.example/ ,")
	want := []string{"https://a.example/rpc", "https://b.example/rpc", "https://c.example/"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SplitEndpoints mismatch: got %v want %v", got, want)
	}

	if got := SplitEndpoints(""); len(got) != 0 {
		t.Fatalf("empty config must yield no endpoints, got %v", got)
	}

	if got := SplitEndpoints("grpc.trongrid.io:50051"); len(got) != 1 || got[0] != "grpc.trongrid.io:50051" {
		t.Fatalf("single host:port must be kept verbatim, got %v", got)
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	s := "回调地址已拦截" // 每个汉字 3 字节
	for n := 0; n <= len(s); n++ {
		got := truncate(s, n)
		if len(got) > n || !utf8.ValidString(got) {
			t.Fatalf("truncate(%d) = %q (len %d) must be valid UTF-8 within limit", n, got, len(got))
		}
	}
	if truncate("abc", 10) != "abc" {
		t.Fatal("short strings must be unchanged")
	}
}
