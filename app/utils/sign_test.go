package utils

import "testing"

func TestSignEqual(t *testing.T) {
	sig := EpusdtSign(map[string]interface{}{"order_id": "1", "amount": 42}, "token")

	if !SignEqual(sig, sig) {
		t.Fatal("identical signature must match")
	}
	for _, got := range []any{nil, "", 123, sig[:len(sig)-1], sig + "0"} {
		if SignEqual(sig, got) {
			t.Fatalf("SignEqual must reject %#v", got)
		}
	}
	if SignEqual("", "") {
		t.Fatal("empty expected signature must never match")
	}
}
