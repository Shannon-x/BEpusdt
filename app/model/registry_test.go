package model

import (
	"reflect"
	"testing"
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
