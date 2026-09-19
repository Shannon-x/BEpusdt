package cmd

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/model/migration"
)

func TestCopyTableMovesRowsBetweenDatabasesIdempotently(t *testing.T) {
	dir := t.TempDir()
	src, err := model.Open(model.DriverSQLite, filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	dst, err := model.Open(model.DriverSQLite, filepath.Join(dir, "dst.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migration.Run(src, model.Models()); err != nil {
		t.Fatal(err)
	}
	if _, err := migration.Run(dst, model.Models()); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	for i := 1; i <= 1203; i++ {
		w := model.Wallet{Name: "w", Status: 1, Address: "0xabc" + itoa(i), MatchAddr: "m" + itoa(i), TradeType: string(model.UsdtPolygon)}
		if err := src.Create(&w).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Create(&model.Conf{K: model.PaymentTimeout, V: "1200"}).Error; err != nil {
		t.Fatal(err)
	}
	confirmed := now
	order := model.Order{OrderId: "m1", TradeId: "t1", TradeType: model.UsdtPolygon, Fiat: "CNY", Crypto: "USDT", Rate: "7", Amount: "1", Money: "7", Address: "a", Status: 2, ExpiredAt: now, ConfirmedAt: &confirmed}
	if err := src.Create(&order).Error; err != nil {
		t.Fatal(err)
	}

	for _, m := range model.Models() {
		if _, err := copyTable(src, dst, m, 500); err != nil {
			t.Fatal(err)
		}
	}
	// 重复执行：主键冲突忽略，不报错、不重复
	for _, m := range model.Models() {
		if _, err := copyTable(src, dst, m, 500); err != nil {
			t.Fatal(err)
		}
	}

	var wallets, orders, confs int64
	dst.Model(&model.Wallet{}).Count(&wallets)
	dst.Model(&model.Order{}).Count(&orders)
	dst.Model(&model.Conf{}).Count(&confs)
	if wallets != 1203 || orders != 1 || confs != 1 {
		t.Fatalf("copied counts mismatch: wallets=%d orders=%d confs=%d", wallets, orders, confs)
	}
	var got model.Order
	dst.Where("trade_id = ?", "t1").Find(&got)
	if got.ID != order.ID || got.Status != 2 {
		t.Fatalf("primary keys and fields must be preserved, got %+v", got)
	}
}

func itoa(i int) string {
	return string(rune('0'+i/1000%10)) + string(rune('0'+i/100%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10))
}
