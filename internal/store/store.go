// Package store is the connector's durable SQLite state: uploads, media, job queue and outbox; no secrets.
package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS uploads (
  id TEXT PRIMARY KEY,
  media_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  token_sha256 TEXT NOT NULL,
  ext TEXT NOT NULL,
  declared_size INTEGER NOT NULL,
  received INTEGER NOT NULL DEFAULT 0,
  thumbnail_at REAL,
  status TEXT NOT NULL DEFAULT 'open',
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status, expires_at);

CREATE TABLE IF NOT EXISTS media (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL DEFAULT '',
  ext TEXT NOT NULL,
  status TEXT NOT NULL,
  stage TEXT NOT NULL DEFAULT 'received',
  progress REAL NOT NULL DEFAULT 0,
  file_size INTEGER NOT NULL DEFAULT 0,
  duration REAL,
  width INTEGER,
  height INTEGER,
  fps REAL,
  has_audio INTEGER NOT NULL DEFAULT 0,
  requested_thumb_at REAL,
  thumb_at REAL,
  probe_json TEXT,
  nsfw_json TEXT,
  hls_json TEXT,
  sonic_json TEXT,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_media_status ON media(status);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  media_id TEXT NOT NULL,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  worker_id TEXT,
  lease_expires_at INTEGER,
  progress REAL,
  processed_frames INTEGER,
  total_frames INTEGER,
  error TEXT,
  result_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(job_type, status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_media ON jobs(media_id);

CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

// ErrNotFound is returned for a missing row.
var ErrNotFound = errors.New("not found")

// Store wraps the database.
type Store struct {
	db *sql.DB
}

// Open opens (creating when needed) the database at path.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One writer at a time keeps SQLite simple and correct.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// NewID returns a random hex id with a prefix.
func NewID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

func now() int64 { return time.Now().Unix() }

// ---- kv -------------------------------------------------------------------

// Get returns a kv value ("" when unset).
func (s *Store) Get(key string) string {
	var v string
	_ = s.db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&v)
	return v
}

// Set stores a kv value.
func (s *Store) Set(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// ---- uploads --------------------------------------------------------------

// Upload is one expected or in-flight upload.
type Upload struct {
	ID           string
	MediaID      string
	Kind         string
	TokenSHA256  string
	Ext          string
	DeclaredSize int64
	Received     int64
	ThumbnailAt  *float64
	Status       string
	ExpiresAt    time.Time
}

// ExpectUpload records an announced upload; re-announcing the same id updates it.
func (s *Store) ExpectUpload(u Upload) error {
	t := now()
	_, err := s.db.Exec(`
		INSERT INTO uploads(id, media_id, kind, token_sha256, ext, declared_size, thumbnail_at, status, expires_at, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?, 'open', ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET token_sha256 = excluded.token_sha256, expires_at = excluded.expires_at, updated_at = excluded.updated_at
		WHERE uploads.status = 'open'`,
		u.ID, u.MediaID, u.Kind, u.TokenSHA256, u.Ext, u.DeclaredSize, u.ThumbnailAt, u.ExpiresAt.Unix(), t, t)
	return err
}

// GetUpload loads an upload by id.
func (s *Store) GetUpload(id string) (*Upload, error) {
	var u Upload
	var exp int64
	var thumb sql.NullFloat64
	err := s.db.QueryRow(`SELECT id, media_id, kind, token_sha256, ext, declared_size, received, thumbnail_at, status, expires_at
		FROM uploads WHERE id = ?`, id).Scan(&u.ID, &u.MediaID, &u.Kind, &u.TokenSHA256, &u.Ext, &u.DeclaredSize, &u.Received, &thumb, &u.Status, &exp)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if thumb.Valid {
		v := thumb.Float64
		u.ThumbnailAt = &v
	}
	u.ExpiresAt = time.Unix(exp, 0)
	return &u, nil
}

// SetUploadReceived records the byte offset reached.
func (s *Store) SetUploadReceived(id string, received int64) error {
	_, err := s.db.Exec(`UPDATE uploads SET received = ?, updated_at = ? WHERE id = ?`, received, now(), id)
	return err
}

// SetUploadStatus moves an upload to complete/cancelled/failed.
func (s *Store) SetUploadStatus(id, status string) error {
	_, err := s.db.Exec(`UPDATE uploads SET status = ?, updated_at = ? WHERE id = ?`, status, now(), id)
	return err
}

// StaleUploads lists open uploads that expired or were idle for maxIdle.
func (s *Store) StaleUploads(maxIdle time.Duration) ([]Upload, error) {
	rows, err := s.db.Query(`SELECT id, media_id FROM uploads WHERE status = 'open' AND (expires_at < ? OR updated_at < ?)`,
		now(), time.Now().Add(-maxIdle).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Upload
	for rows.Next() {
		var u Upload
		if err := rows.Scan(&u.ID, &u.MediaID); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// OpenUploadStats counts unexpired open uploads and sums their declared and received bytes.
func (s *Store) OpenUploadStats() (n int, declared, received int64, err error) {
	err = s.db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(declared_size),0), COALESCE(SUM(received),0)
		FROM uploads WHERE status = 'open' AND expires_at >= ?`, now()).Scan(&n, &declared, &received)
	return
}

// PruneUploads deletes finished upload rows older than age.
func (s *Store) PruneUploads(age time.Duration) error {
	_, err := s.db.Exec(`DELETE FROM uploads WHERE status != 'open' AND updated_at < ?`, time.Now().Add(-age).Unix())
	return err
}

// ---- media ----------------------------------------------------------------

// Media is one stored item.
type Media struct {
	ID               string
	Type             string
	Ext              string
	Status           string // received | processing | done | failed
	Stage            string
	Progress         float64
	FileSize         int64
	Duration         float64
	Width, Height    int
	FPS              float64
	HasAudio         bool
	RequestedThumbAt *float64
	ThumbAt          *float64
	ProbeJSON        string
	NsfwJSON         string
	HlsJSON          string
	SonicJSON        string
	Error            string
}

// InsertMedia creates a media row (idempotent on id).
func (s *Store) InsertMedia(m Media) error {
	t := now()
	_, err := s.db.Exec(`INSERT INTO media(id, ext, status, stage, file_size, requested_thumb_at, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`,
		m.ID, m.Ext, m.Status, m.Stage, m.FileSize, m.RequestedThumbAt, t, t)
	return err
}

// GetMedia loads one media row.
func (s *Store) GetMedia(id string) (*Media, error) {
	var m Media
	var dur, fps, req, thumb sql.NullFloat64
	var w, h sql.NullInt64
	var hasAudio int
	var probe, nsfw, hls, sonic, errStr sql.NullString
	err := s.db.QueryRow(`SELECT id, type, ext, status, stage, progress, file_size, duration, width, height, fps, has_audio,
		requested_thumb_at, thumb_at, probe_json, nsfw_json, hls_json, sonic_json, error FROM media WHERE id = ?`, id).
		Scan(&m.ID, &m.Type, &m.Ext, &m.Status, &m.Stage, &m.Progress, &m.FileSize, &dur, &w, &h, &fps, &hasAudio,
			&req, &thumb, &probe, &nsfw, &hls, &sonic, &errStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	m.Duration, m.FPS = dur.Float64, fps.Float64
	m.Width, m.Height = int(w.Int64), int(h.Int64)
	m.HasAudio = hasAudio == 1
	if req.Valid {
		v := req.Float64
		m.RequestedThumbAt = &v
	}
	if thumb.Valid {
		v := thumb.Float64
		m.ThumbAt = &v
	}
	m.ProbeJSON, m.NsfwJSON, m.HlsJSON, m.SonicJSON, m.Error = probe.String, nsfw.String, hls.String, sonic.String, errStr.String
	return &m, nil
}

// UpdateMediaProbe records classification and probe results.
func (s *Store) UpdateMediaProbe(id, typ string, size int64, duration float64, w, h int, fps float64, hasAudio bool, probeJSON string) error {
	ha := 0
	if hasAudio {
		ha = 1
	}
	_, err := s.db.Exec(`UPDATE media SET type = ?, file_size = ?, duration = ?, width = ?, height = ?, fps = ?, has_audio = ?, probe_json = ?, updated_at = ? WHERE id = ?`,
		typ, size, duration, w, h, fps, ha, probeJSON, now(), id)
	return err
}

// SetMediaStage records the current stage and overall progress (never backwards).
func (s *Store) SetMediaStage(id, status, stage string, progress float64) error {
	_, err := s.db.Exec(`UPDATE media SET status = ?, stage = ?, progress = MAX(progress, ?), updated_at = ? WHERE id = ?`,
		status, stage, progress, now(), id)
	return err
}

// SetMediaField stores one of the JSON result columns or thumb_at.
func (s *Store) SetMediaResult(id, column string, value any) error {
	switch column {
	case "nsfw_json", "hls_json", "sonic_json", "thumb_at", "requested_thumb_at", "error":
	default:
		return fmt.Errorf("unknown media column %q", column)
	}
	// column is one of the names allowed above.
	_, err := s.db.Exec(`UPDATE media SET `+column+ //nolint:gosec
		` = ?, updated_at = ? WHERE id = ?`, value, now(), id)
	return err
}

// DeleteMedia removes a media row and its jobs.
func (s *Store) DeleteMedia(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM jobs WHERE media_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM media WHERE id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE uploads SET status = 'cancelled', updated_at = ? WHERE media_id = ? AND status = 'open'`, now(), id); err != nil {
		return err
	}
	return tx.Commit()
}

// MediaIDsWithStatus lists media ids in the given statuses.
func (s *Store) MediaIDsWithStatus(statuses ...string) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	q := `SELECT id FROM media WHERE status IN (?` + //nolint:gosec // only placeholders are appended
		repeat(",?", len(statuses)-1) + `) ORDER BY created_at`
	args := make([]any, len(statuses))
	for i, st := range statuses {
		args[i] = st
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// UnfinishedMediaBytes sums originals waiting for or in processing (their output is not on disk yet).
func (s *Store) UnfinishedMediaBytes() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(file_size),0) FROM media WHERE status IN ('received', 'processing')`).Scan(&n)
	return n, err
}

// MediaStats returns total stored bytes of originals and counts by status.
func (s *Store) MediaStats() (bytes int64, counts map[string]int, err error) {
	counts = map[string]int{}
	if err = s.db.QueryRow(`SELECT COALESCE(SUM(file_size),0) FROM media`).Scan(&bytes); err != nil {
		return
	}
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM media GROUP BY status`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int
		if err = rows.Scan(&st, &n); err != nil {
			return
		}
		counts[st] = n
	}
	err = rows.Err()
	return
}

// ScanWaiting counts media at nsfw_scan with no scan running (queued, or job not created yet).
func (s *Store) ScanWaiting(jobType string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM media m WHERE m.status = 'processing' AND m.stage = 'nsfw_scan'
		AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.media_id = m.id AND j.job_type = ? AND j.status = 'processing')`, jobType).Scan(&n)
	return n, err
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

// ---- jobs (NSFW worker queue) ---------------------------------------------

// Job is one queued unit of AI work.
type Job struct {
	ID          string
	MediaID     string
	JobType     string
	Status      string
	Attempts    int
	MaxAttempts int
	WorkerID    string
	ResultJSON  string
	Error       string
}

// EnqueueJob queues a job unless an open one exists for the media.
func (s *Store) EnqueueJob(mediaID, jobType string, maxAttempts int) (string, error) {
	var existing string
	err := s.db.QueryRow(`SELECT id FROM jobs WHERE media_id = ? AND job_type = ? AND status IN ('queued','processing')`, mediaID, jobType).Scan(&existing)
	if err == nil {
		return existing, nil
	}
	id := NewID("j_")
	t := now()
	_, err = s.db.Exec(`INSERT INTO jobs(id, media_id, job_type, status, max_attempts, created_at, updated_at) VALUES(?,?,?,'queued',?,?,?)`,
		id, mediaID, jobType, maxAttempts, t, t)
	return id, err
}

// ClaimJob leases the oldest queued job of a type to a worker.
func (s *Store) ClaimJob(jobType, workerID string, lease time.Duration) (*Job, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var j Job
	err = tx.QueryRow(`SELECT id, media_id, job_type, attempts, max_attempts FROM jobs WHERE job_type = ? AND status = 'queued' ORDER BY created_at LIMIT 1`, jobType).
		Scan(&j.ID, &j.MediaID, &j.JobType, &j.Attempts, &j.MaxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j.Attempts++
	j.Status = "processing"
	j.WorkerID = workerID
	if _, err := tx.Exec(`UPDATE jobs SET status = 'processing', attempts = ?, worker_id = ?, lease_expires_at = ?, updated_at = ? WHERE id = ?`,
		j.Attempts, workerID, time.Now().Add(lease).Unix(), now(), j.ID); err != nil {
		return nil, err
	}
	return &j, tx.Commit()
}

// GetJob loads a job.
func (s *Store) GetJob(id string) (*Job, error) {
	var j Job
	var worker, result, errStr sql.NullString
	err := s.db.QueryRow(`SELECT id, media_id, job_type, status, attempts, max_attempts, worker_id, result_json, error FROM jobs WHERE id = ?`, id).
		Scan(&j.ID, &j.MediaID, &j.JobType, &j.Status, &j.Attempts, &j.MaxAttempts, &worker, &result, &errStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	j.WorkerID, j.ResultJSON, j.Error = worker.String, result.String, errStr.String
	return &j, nil
}

// HeartbeatJob renews the lease when the worker still owns the job.
func (s *Store) HeartbeatJob(id, workerID string, lease time.Duration, progress *float64, processed, total *int) (bool, error) {
	res, err := s.db.Exec(`UPDATE jobs SET lease_expires_at = ?, progress = COALESCE(?, progress), processed_frames = COALESCE(?, processed_frames),
		total_frames = COALESCE(?, total_frames), updated_at = ? WHERE id = ? AND status = 'processing' AND worker_id = ?`,
		time.Now().Add(lease).Unix(), progress, processed, total, now(), id, workerID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// CompleteJob finishes a job owned by the worker.
func (s *Store) CompleteJob(id, workerID, resultJSON string) (bool, error) {
	res, err := s.db.Exec(`UPDATE jobs SET status = 'completed', result_json = ?, lease_expires_at = NULL, updated_at = ? WHERE id = ? AND status = 'processing' AND worker_id = ?`,
		resultJSON, now(), id, workerID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// FailJob requeues while attempts remain and it is retryable, else fails for good; returns whether requeued.
func (s *Store) FailJob(id, workerID, msg string, retryable bool) (requeued bool, owned bool, err error) {
	j, err := s.GetJob(id)
	if err != nil {
		return false, false, err
	}
	if j.Status != "processing" || j.WorkerID != workerID {
		return false, false, nil
	}
	status := "failed"
	if retryable && j.Attempts < j.MaxAttempts {
		status = "queued"
	}
	_, err = s.db.Exec(`UPDATE jobs SET status = ?, error = ?, worker_id = NULL, lease_expires_at = NULL, updated_at = ? WHERE id = ?`,
		status, truncate(msg, 2000), now(), id)
	return status == "queued", true, err
}

// RequeueJob returns a job to the queue without counting the attempt.
func (s *Store) RequeueJob(id, workerID string) error {
	_, err := s.db.Exec(`UPDATE jobs SET status = 'queued', attempts = MAX(attempts - 1, 0), worker_id = NULL,
		lease_expires_at = NULL, updated_at = ? WHERE id = ? AND status = 'processing' AND worker_id = ?`, now(), id, workerID)
	return err
}

// RecoverJobs requeues (or fails, when out of attempts) jobs whose lease expired.
func (s *Store) RecoverJobs() (requeued, failed int, err error) {
	t := now()
	res, err := s.db.Exec(`UPDATE jobs SET status = 'queued', worker_id = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE status = 'processing' AND lease_expires_at < ? AND attempts < max_attempts`, t, t)
	if err != nil {
		return
	}
	n, _ := res.RowsAffected()
	requeued = int(n)
	res, err = s.db.Exec(`UPDATE jobs SET status = 'failed', error = COALESCE(error, 'lease expired'), worker_id = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE status = 'processing' AND lease_expires_at < ?`, t, t)
	if err != nil {
		return
	}
	n, _ = res.RowsAffected()
	failed = int(n)
	return
}

// ReleaseAllProcessing requeues every in-flight job at boot, when no worker can hold a lease.
func (s *Store) ReleaseAllProcessing() error {
	_, err := s.db.Exec(`UPDATE jobs SET status = 'queued', worker_id = NULL, lease_expires_at = NULL, updated_at = ? WHERE status = 'processing'`, now())
	return err
}

// JobCounts returns queued/processing/completed/failed counts.
func (s *Store) JobCounts() (map[string]int, error) {
	out := map[string]int{"queued": 0, "processing": 0, "completed": 0, "failed": 0}
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return out, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// LatestJob returns the most recent job of a type for a media item.
func (s *Store) LatestJob(mediaID, jobType string) (*Job, error) {
	var id string
	err := s.db.QueryRow(`SELECT id FROM jobs WHERE media_id = ? AND job_type = ? ORDER BY created_at DESC, rowid DESC LIMIT 1`, mediaID, jobType).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.GetJob(id)
}

// PruneJobs deletes terminal jobs older than age.
func (s *Store) PruneJobs(age time.Duration) error {
	_, err := s.db.Exec(`DELETE FROM jobs WHERE status IN ('completed','failed') AND updated_at < ?`, time.Now().Add(-age).Unix())
	return err
}

// ---- outbox ---------------------------------------------------------------

// OutboxItem is a report waiting for the server's acknowledgement.
type OutboxItem struct {
	ID      string
	Type    string
	Payload string
}

// Enqueue adds a message to the outbox; its id is the WS message id so the server can deduplicate.
func (s *Store) Enqueue(typ, payload string) (string, error) {
	id := NewID("o_")
	_, err := s.db.Exec(`INSERT INTO outbox(id, type, payload, created_at) VALUES(?,?,?,?)`, id, typ, payload, time.Now().UnixNano())
	return id, err
}

// Outbox returns pending messages, oldest first.
func (s *Store) Outbox(limit int) ([]OutboxItem, error) {
	rows, err := s.db.Query(`SELECT id, type, payload FROM outbox ORDER BY created_at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxItem
	for rows.Next() {
		var it OutboxItem
		if err := rows.Scan(&it.ID, &it.Type, &it.Payload); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// Ack removes an acknowledged message.
func (s *Store) Ack(id string) error {
	_, err := s.db.Exec(`DELETE FROM outbox WHERE id = ?`, id)
	return err
}

// BumpAttempt counts a failed delivery; poison messages are dropped after max.
func (s *Store) BumpAttempt(id string, max int) (dropped bool, err error) {
	if _, err = s.db.Exec(`UPDATE outbox SET attempts = attempts + 1 WHERE id = ?`, id); err != nil {
		return
	}
	res, err := s.db.Exec(`DELETE FROM outbox WHERE id = ? AND attempts >= ?`, id, max)
	if err != nil {
		return
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// OutboxLen counts pending messages.
func (s *Store) OutboxLen() int {
	var n int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n)
	return n
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
