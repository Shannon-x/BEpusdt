package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/v03413/bepusdt/app/model"
	"gorm.io/gorm"
)

func failScanJobCreates(t *testing.T) func() {
	t.Helper()
	name := "test_fail_scan_job_create_" + t.Name()
	if err := model.Db.Callback().Create().Before("gorm:create").Register(name, func(db *gorm.DB) {
		if db.Statement.Table == (model.ScanJob{}).TableName() {
			db.AddError(errors.New("injected scan job create failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	restore := func() { model.Db.Callback().Create().Remove(name) }
	t.Cleanup(restore)

	return restore
}

func TestReplayTracksConsumersBeforeDispatchCompletes(t *testing.T) {
	const network = "early-consumer"
	registerScanner(network, func() ScanStatus { return ScanStatus{Network: network} }, func(from, to, jobID int64) int {
		for h := from; h <= to; h++ {
			scanJobPartQueued(jobID, h)
			scanJobPartDone(jobID, h)
			scanJobPartDone(jobID, h) // 重复完成不得提前结算或多减一次计数
			job, _ := model.GetScanJob(jobID)
			if job.Status != model.ScanJobStatusRunning {
				t.Fatal("replay must stay running until dispatch is sealed")
			}
		}

		return int(to - from + 1)
	})
	if _, err := Replay(network, 10, 12); err != nil {
		t.Fatal(err)
	}
	job := model.RecentScanJobs(network, 1)[0]
	if job.Status != model.ScanJobStatusDone {
		t.Fatalf("all early completions must be counted after sealing, got %s", job.Status)
	}
}

func TestDuplicateJobPartDoesNotCompleteAnotherPart(t *testing.T) {
	job, err := model.CreateScanJob("duplicate-part", 1, 2, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	scanJobBegin(job, 2)
	scanJobPartQueued(job.ID, 1)
	scanJobPartQueued(job.ID, 2)
	scanJobPartDone(job.ID, 1)
	scanJobPartDone(job.ID, 1)
	if got, _ := model.GetScanJob(job.ID); got.Status != model.ScanJobStatusRunning {
		t.Fatalf("duplicate completion must not complete the pending part, got %s", got.Status)
	}
	scanJobPartDone(job.ID, 2)
	if got, _ := model.GetScanJob(job.ID); got.Status != model.ScanJobStatusDone {
		t.Fatalf("distinct completed parts must settle the job, got %s", got.Status)
	}
}

func TestAbandonDatabaseFailureKeepsCursorUntilJobIsDurable(t *testing.T) {
	const network = "abandon-db-failure"
	c := cursorOf(network)
	c.reset(100)
	c.issue(101, 103)
	c.complete(102, 103)
	restore := failScanJobCreates(t)
	scanAbandon(network, 101, 101, 0, "RPC failed", scanLogger(network, "https://example", "test", nil))
	if c.current() != 100 || c.pendingCount() != 3 {
		t.Fatalf("undurable abandoned block must retain cursor barrier, height=%d pending=%d", c.current(), c.pendingCount())
	}
	if len(model.RecentScanJobs(network, 1)) != 0 {
		t.Fatal("fault injection must prevent durable abandoned job creation")
	}
	restore()
	retryAbandonedRecords()
	if c.current() != 103 || c.pendingCount() != 0 {
		t.Fatalf("durable abandoned job may release the cursor barrier, height=%d pending=%d", c.current(), c.pendingCount())
	}
	if jobs := model.RecentScanJobs(network, 2); len(jobs) != 1 || jobs[0].Status != model.ScanJobStatusPending {
		t.Fatalf("failed create must be retried as exactly one pending job, got %+v", jobs)
	}
}

func TestGapDatabaseFailureDoesNotResetCursorOrPendingBlocks(t *testing.T) {
	const network = "gap-db-failure"
	c := cursorOf(network)
	c.reset(100)
	c.issue(101, 200)
	restore := failScanJobCreates(t)
	last, err := resumeFrom(network, 200, 5000, 1000)
	if err == nil || last != 200 || c.current() != 100 || c.pendingCount() != 100 {
		t.Fatalf("gap persistence failure must retain issued and completed boundaries: last=%d height=%d pending=%d err=%v", last, c.current(), c.pendingCount(), err)
	}
	restore()
	last, err = resumeFrom(network, 200, 5000, 1000)
	if err != nil || last != 4999 {
		t.Fatalf("gap persistence recovery must allow alignment, last=%d err=%v", last, err)
	}
	jobs := model.RecentScanJobs(network, 1)
	if len(jobs) != 1 || jobs[0].FromHeight != 101 || jobs[0].ToHeight != 4999 {
		t.Fatalf("gap must also cover in-flight blocks cleared by reset, got %+v", jobs)
	}
}

func TestCursorReadFailureIsRetriedInsteadOfTreatedAsFreshStart(t *testing.T) {
	const network = "cursor-read-fail"
	if err := model.SaveScanCursor(network, 100); err != nil {
		t.Fatal(err)
	}
	name := "test_cursor_read_failure"
	if err := model.Db.Callback().Query().Before("gorm:query").Register(name, func(db *gorm.DB) {
		if db.Statement.Table == (model.ScanCursor{}).TableName() {
			db.AddError(errors.New("injected cursor read failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { model.Db.Callback().Query().Remove(name) })
	if _, err := resumeFrom(network, 0, 150, 1000); err == nil {
		t.Fatal("cursor read failure must stop head alignment")
	}
	model.Db.Callback().Query().Remove(name)
	if last, err := resumeFrom(network, 0, 150, 1000); err != nil || last != 100 {
		t.Fatalf("successful next read must recover the saved cursor, last=%d err=%v", last, err)
	}
}

func TestFastRestartImmediatelyRecoversRecentRunningJobs(t *testing.T) {
	job, err := model.CreateScanJob("fast-restart", 1, 2, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.RecoverScanJobs(0, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := model.GetScanJob(job.ID); got.Status != model.ScanJobStatusPending || got.NextRetryAt.After(time.Now()) {
		t.Fatalf("recent running job must be immediately eligible after restart, got %+v", got)
	}
}

func TestPeriodicRecoveryExcludesCurrentlyTrackedJobs(t *testing.T) {
	create := func(network string) model.ScanJob {
		job, err := model.CreateScanJob(network, 1, 1, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		model.Db.Model(&job).UpdateColumn("updated_at", time.Now().Add(-time.Hour))

		return job
	}
	tracked := create("active-running")
	untracked := create("lost-running")
	scanJobBegin(tracked, 1)
	t.Cleanup(func() { scanJobs.Delete(tracked.ID) })
	scanJobRetry(context.Background())
	if job, _ := model.GetScanJob(tracked.ID); job.Status != model.ScanJobStatusRunning {
		t.Fatal("currently tracked job must stay running despite its age")
	}
	if job, _ := model.GetScanJob(untracked.ID); job.Status != model.ScanJobStatusPending {
		t.Fatal("old untracked running job must be periodically recovered")
	}
}

func TestJobCompletionDatabaseFailureIsRetried(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			job, err := model.CreateScanJob("completion-db", 10, 10, model.ScanJobKindReplay, model.ScanJobStatusRunning, "", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			scanJobBegin(job, 1)
			name := "test_fail_scan_completion_" + t.Name()
			if err := model.Db.Callback().Update().Before("gorm:update").Register(name, func(db *gorm.DB) {
				if db.Statement.Table == (model.ScanJob{}).TableName() {
					db.AddError(errors.New("injected job completion update failure"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { model.Db.Callback().Update().Remove(name) })
			if failure {
				scanJobPartFailed(job.ID, "block unavailable")
			} else {
				scanJobPartDone(job.ID)
			}
			if got, _ := model.GetScanJob(job.ID); got.Status != model.ScanJobStatusRunning || got.Attempts != 0 {
				t.Fatalf("failed state update must leave durable state unchanged, got %+v", got)
			}
			if _, tracked := scanJobs.Load(job.ID); !tracked {
				t.Fatal("completion must stay tracked until durable state update succeeds")
			}
			model.Db.Callback().Update().Remove(name)
			scanJobRetry(context.Background())
			got, _ := model.GetScanJob(job.ID)
			want := model.ScanJobStatusDone
			if failure {
				want = model.ScanJobStatusPending
				if got.Attempts != 1 || got.LastError != "block unavailable" {
					t.Fatalf("failed settlement retry must increment attempts once, got %+v", got)
				}
			}
			if got.Status != want {
				t.Fatalf("completion update must recover after database outage, got %s want %s", got.Status, want)
			}
			if _, tracked := scanJobs.Load(job.ID); tracked {
				t.Fatal("durable settled job must be removed from progress tracking")
			}
		})
	}
}

func TestDeferredJobRetryPagesLargeRangeAndPersistsProgress(t *testing.T) {
	const network = "paged-recovery"
	job, err := model.CreateScanJob(network, 1, replayMaxBlocks+7, model.ScanJobKindGap, model.ScanJobStatusDeferred, "prior gap", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := model.RetryScanJobs(network, []string{model.ScanJobStatusDeferred}, job.ID); err != nil || n != 1 {
		t.Fatalf("manual retry must enqueue the deferred task, n=%d err=%v", n, err)
	}
	var ranges [][2]int64
	registerScanner(network, func() ScanStatus { return ScanStatus{Network: network} }, func(from, to, jobID int64) int {
		ranges = append(ranges, [2]int64{from, to})
		scanJobPartQueued(jobID, from)
		scanJobPartDone(jobID, from)

		return 1
	})
	scanJobRetry(context.Background())
	first, _ := model.GetScanJob(job.ID)
	if len(ranges) != 1 || ranges[0] != [2]int64{1, replayMaxBlocks} || first.Status != model.ScanJobStatusPending || first.NextHeight != replayMaxBlocks+1 {
		t.Fatalf("large range must checkpoint the first bounded page, ranges=%v job=%+v", ranges, first)
	}
	scanJobRetry(context.Background())
	last, _ := model.GetScanJob(job.ID)
	if len(ranges) != 2 || ranges[1] != [2]int64{replayMaxBlocks + 1, replayMaxBlocks + 7} || last.Status != model.ScanJobStatusDone {
		t.Fatalf("next cycle must resume the remaining page, ranges=%v job=%+v", ranges, last)
	}
	if last.FromHeight != 1 || last.ToHeight != replayMaxBlocks+7 {
		t.Fatal("page checkpoints must retain the original recovery range")
	}
}

func TestLatePreviousPageCompletionCannotCompleteNewPage(t *testing.T) {
	const network = "late-paged-complete"
	job, err := model.CreateScanJob(network, 1, replayMaxBlocks+1, model.ScanJobKindGap, model.ScanJobStatusPending, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	registerScanner(network, func() ScanStatus { return ScanStatus{Network: network} }, func(from, to, jobID int64) int {
		if from == 1 {
			scanJobPartQueued(jobID, from)
			scanJobPartDone(jobID, from)
		} else {
			// 上一页重发的回报可能恰好出现在新页开始注册批次之前。
			scanJobPartDone(jobID, 1)
			scanJobPartQueued(jobID, from)
		}

		return 1
	})
	scanJobRetry(context.Background())
	scanJobRetry(context.Background())
	if got, _ := model.GetScanJob(job.ID); got.Status != model.ScanJobStatusRunning {
		t.Fatalf("old page completion must not settle an unscanned new page, got %+v", got)
	}
	scanJobPartDone(job.ID, replayMaxBlocks+1)
	if got, _ := model.GetScanJob(job.ID); got.Status != model.ScanJobStatusDone {
		t.Fatalf("new page must settle only on its own registered completion, got %+v", got)
	}
}
