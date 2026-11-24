package store

import (
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	s, err := Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestJobLifecycle(t *testing.T) {
	s := open(t)
	id, err := s.EnqueueJob("m1", "nsfw_scan", 2)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := s.EnqueueJob("m1", "nsfw_scan", 2); again != id {
		t.Fatal("an open job must not be duplicated")
	}
	j, err := s.ClaimJob("nsfw_scan", "w1", time.Minute)
	if err != nil || j == nil || j.ID != id || j.Attempts != 1 {
		t.Fatalf("claim: %+v %v", j, err)
	}
	if j2, _ := s.ClaimJob("nsfw_scan", "w2", time.Minute); j2 != nil {
		t.Fatal("claimed twice")
	}
	if ok, _ := s.CompleteJob(id, "w2", "{}"); ok {
		t.Fatal("non-owner completed the job")
	}
	requeued, owned, _ := s.FailJob(id, "w1", "boom", true)
	if !requeued || !owned {
		t.Fatal("first failure should requeue")
	}
	j, _ = s.ClaimJob("nsfw_scan", "w1", time.Minute)
	requeued, _, _ = s.FailJob(j.ID, "w1", "boom", true)
	if requeued {
		t.Fatal("out of attempts: must fail for good")
	}
	got, _ := s.GetJob(id)
	if got.Status != "failed" {
		t.Fatalf("status %s", got.Status)
	}
}

func TestOutboxOrderAndAck(t *testing.T) {
	s := open(t)
	a, _ := s.Enqueue("media.probed", `{"n":1}`)
	b, _ := s.Enqueue("media.processed", `{"n":2}`)
	items, _ := s.Outbox(10)
	if len(items) != 2 || items[0].ID != a || items[1].ID != b {
		t.Fatalf("order: %+v", items)
	}
	_ = s.Ack(a)
	if s.OutboxLen() != 1 {
		t.Fatal("ack did not remove")
	}
	for i := 0; i < 2; i++ {
		dropped, _ := s.BumpAttempt(b, 2)
		if dropped != (i == 1) {
			t.Fatalf("drop on attempt %d = %v", i, dropped)
		}
	}
}

func TestUploadExpectIdempotent(t *testing.T) {
	s := open(t)
	u := Upload{ID: "up_12345678", MediaID: "m1", Kind: "media", TokenSHA256: "aa", Ext: ".mp4", DeclaredSize: 10, ExpiresAt: time.Now().Add(time.Hour)}
	if err := s.ExpectUpload(u); err != nil {
		t.Fatal(err)
	}
	u.TokenSHA256 = "bb"
	if err := s.ExpectUpload(u); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetUpload(u.ID)
	if got.TokenSHA256 != "bb" {
		t.Fatal("re-announce should refresh the token")
	}
}

func TestDiskAccounting(t *testing.T) {
	s := open(t)
	exp := func(id string, size int64, in time.Duration) {
		if err := s.ExpectUpload(Upload{ID: id, MediaID: "m_" + id, Kind: "media", TokenSHA256: "aa", Ext: ".mp4", DeclaredSize: size, ExpiresAt: time.Now().Add(in)}); err != nil {
			t.Fatal(err)
		}
	}
	exp("up_aaaaaaaa", 100, time.Hour)
	exp("up_bbbbbbbb", 200, time.Hour)
	exp("up_expired0", 400, -time.Minute)
	exp("up_finished", 800, time.Hour)
	_ = s.SetUploadReceived("up_aaaaaaaa", 30)
	_ = s.SetUploadStatus("up_finished", "complete")
	n, declared, received, err := s.OpenUploadStats()
	if err != nil || n != 2 || declared != 300 || received != 30 {
		t.Fatalf("open uploads: %d %d %d %v", n, declared, received, err)
	}
	for id, st := range map[string]string{"m_recv": "received", "m_proc": "processing", "m_done": "done", "m_fail": "failed"} {
		if err := s.InsertMedia(Media{ID: id, Ext: ".mp4", Status: st, Stage: "received", FileSize: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := s.UnfinishedMediaBytes(); err != nil || b != 2000 {
		t.Fatalf("unfinished media bytes: %d %v", b, err)
	}
}
