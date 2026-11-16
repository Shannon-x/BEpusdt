package notify

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"path/filepath"

	"github.com/glebarez/sqlite"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/netguard"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// useGlobalTestDB 把 model.Db 指向临时库并完成全量迁移，Handle 依赖全局 Db 与配置缓存
func useGlobalTestDB(t *testing.T, allowPrivate string) {
	t.Helper()

	initNotifyTestLog(t)
	// 不限制连接数：SetK 会在事务内调用 RefreshC，单连接池会死锁
	dbPath := filepath.Join(t.TempDir(), "notify-guard.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?mode=rwc&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	old := model.Db
	model.Db = db
	t.Cleanup(func() {
		model.Db = old
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	if err := model.AutoMigrate(); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	model.FillDefaultConf()
	model.SetK(model.ApiCallbackAllowPrivate, allowPrivate)
	model.RefreshC()
}

func merchantServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	return srv, &hits
}

func createSuccessOrder(t *testing.T, apiType, notifyURL string) model.Order {
	t.Helper()

	o := newWaitingOrder(notifyURL)
	o.ApiType = apiType
	o.Status = model.OrderStatusSuccess
	if err := model.Db.Create(&o).Error; err != nil {
		t.Fatalf("create order: %v", err)
	}

	return o
}

func TestHandleBlocksCallbackToLoopbackByDefault(t *testing.T) {
	useGlobalTestDB(t, "0")
	srv, hits := merchantServer(t)
	o := createSuccessOrder(t, model.OrderApiTypeEpusdt, srv.URL)

	err := Handle(o)
	if !netguard.IsBlocked(err) {
		t.Fatalf("callback to loopback must be blocked by default, got %v", err)
	}
	if hits.Load() != 0 {
		t.Fatal("blocked callback must never reach the target")
	}

	row, ok := model.GetNotifyOutbox(o.ID)
	if !ok || row.Status == model.NotifyOutboxSent || row.LastError == "" {
		t.Fatalf("blocked callback must be recorded as a failure in outbox, got %+v", row)
	}
}

func TestHandleAllowsLoopbackWhenEnabled(t *testing.T) {
	useGlobalTestDB(t, "1")
	srv, hits := merchantServer(t)
	o := createSuccessOrder(t, model.OrderApiTypeEpusdt, srv.URL)

	if err := Handle(o); err != nil {
		t.Fatalf("callback should succeed when private callbacks are allowed: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("merchant should receive exactly one callback, got %d", hits.Load())
	}
}

func TestHandleAdminOrderSkipsSelfCallback(t *testing.T) {
	useGlobalTestDB(t, "0")
	srv, hits := merchantServer(t)
	o := createSuccessOrder(t, model.OrderApiTypeAdmin, srv.URL+"/api/v1/pay/notify")

	if err := Handle(o); err != nil {
		t.Fatalf("admin order callback must succeed without HTTP: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatal("admin order must not issue an HTTP callback to the gateway itself")
	}

	row, ok := model.GetNotifyOutbox(o.ID)
	if !ok || row.Status != model.NotifyOutboxSent {
		t.Fatalf("admin order outbox must be marked sent, got %+v", row)
	}
}
