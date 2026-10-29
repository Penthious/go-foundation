package pubsub

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Penthious/go-foundation/internal/schema"
)

// FailedSchema returns the initial failed-message migration. Apply it through
// service migrations, or use migrate.Config.FailedMessagesVersion. No DDL runs automatically.
func FailedSchema() string {
	return schema.FailedMessages()
}

// FailureState is the durable disposition of a failed delivery.
type FailureState string

const (
	FailureStored        FailureState = "failed"
	FailureReplayPending FailureState = "pending"
	FailureReplayed      FailureState = "replayed"
)

// ErrFailedNotFound indicates an unknown failed-message record.
var ErrFailedNotFound = errors.New("pubsub: failed message not found")

// ErrAlreadyReplayed prevents accidentally replaying an already successful replay.
var ErrAlreadyReplayed = errors.New("pubsub: failed message already replayed")

// FailedStoreConfig bounds SQL operations and complete replay attempts.
type FailedStoreConfig struct {
	// Timeout defaults to 5s for storage operations.
	Timeout time.Duration
	// ReplayTimeout defaults to 30s for claim, handler, and finalization. Handlers
	// must honor cancellation. A claim's lease lasts twice this duration.
	ReplayTimeout time.Duration
}

// FailedMessage retains payload and diagnostics until explicitly deleted after
// successful replay. Raw contains the original transport envelope, including
// malformed data. Replays preserve Message.ID, Topic, and the original Group.
type FailedMessage struct {
	ID             string
	Message        Message
	Group          string
	Raw            []byte
	Reason         string
	LastError      string
	Deliveries     int64
	FailedAt       time.Time
	State          FailureState
	ReplayAttempts int64
	ReplayCount    int64
	ReplayedAt     time.Time
}

// FailedFilter lists one topic/group with stable ID pagination. State is optional.
// Limit defaults to 50 and must be 1-500 when supplied; After is the last seen ID.
type FailedFilter struct {
	Topic string
	Group string
	State FailureState
	After string
	Limit int
}

// FailedStore borrows a PostgreSQL database. It is safe to share and starts no
// workers. Consumers configured with Failures process queued replays themselves.
type FailedStore struct {
	db  *sql.DB
	cfg FailedStoreConfig
}

// NewFailedStore validates config without connecting. Apply FailedSchema before
// consumption. The service owns the database's lifecycle.
func NewFailedStore(db *sql.DB, cfg FailedStoreConfig) (*FailedStore, error) {
	if db == nil || cfg.Timeout < 0 || cfg.ReplayTimeout < 0 {
		return nil, errors.New("pubsub: failed store needs a database and nonnegative timeouts")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	if cfg.ReplayTimeout == 0 {
		cfg.ReplayTimeout = 30 * time.Second
	}

	if cfg.ReplayTimeout < time.Millisecond || cfg.ReplayTimeout > time.Duration(1<<63-1)/2 {
		return nil, errors.New("pubsub: replay timeout must be at least 1ms and fit a doubled lease")
	}

	return &FailedStore{db: db, cfg: cfg}, nil
}

// Ping checks connectivity and required columns, and can be used with health.Required.
func (s *FailedStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	var count int
	err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT "+failureColumns+", available_at FROM public.gof_failed_messages LIMIT 0) AS failures").Scan(&count)
	if err != nil {
		return fmt.Errorf("pubsub: check failed-message schema: %w", err)
	}

	return nil
}

// GetFailed returns a record including its payload and original envelope.
func (s *FailedStore) GetFailed(ctx context.Context, id string) (FailedMessage, error) {
	entry, err := s.get(ctx, id)
	return entry.FailedMessage, err
}

// ListFailed returns a bounded page ordered by ID. Use the last ID as After for
// the next page. This includes bodies; protect access like other application data.
func (s *FailedStore) ListFailed(ctx context.Context, filter FailedFilter) ([]FailedMessage, error) {
	if err := validateTopic(filter.Topic); err != nil {
		return nil, err
	}

	if !validGroup(filter.Group) || (filter.State != "" && filter.State != FailureStored && filter.State != FailureReplayPending && filter.State != FailureReplayed) {
		return nil, errors.New("pubsub: invalid failed-message filter")
	}

	if filter.Limit == 0 {
		filter.Limit = 50
	}

	if filter.Limit < 1 || filter.Limit > 500 || (filter.After != "" && !validFailureID(filter.After)) {
		return nil, errors.New("pubsub: invalid failure page limit or cursor")
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, "SELECT "+failureColumns+` FROM public.gof_failed_messages
		WHERE topic = $1 AND consumer_group = $2 AND ($3 = '' OR state = $3) AND id > $4
		ORDER BY id LIMIT $5`, filter.Topic, filter.Group, filter.State, filter.After, filter.Limit)
	if err != nil {
		return nil, fmt.Errorf("pubsub: list failed messages: %w", err)
	}

	defer rows.Close()
	page := make([]FailedMessage, 0)
	for rows.Next() {
		entry, err := scanFailure(rows)
		if err != nil {
			return nil, err
		}

		page = append(page, entry.FailedMessage)
	}

	return page, rows.Err()
}

// ReplayFailed queues work only for the original topic/group. It is idempotent
// while pending and never republishes to the public topic. Successful records
// cannot be queued again. Malformed envelopes require producer repair instead.
func (s *FailedStore) ReplayFailed(ctx context.Context, id string) error {
	entry, err := s.get(ctx, id)
	if err != nil {
		return err
	}

	if err := entry.Message.Validate(); err != nil {
		return fmt.Errorf("pubsub: invalid archived message cannot be replayed: %w", err)
	}

	if entry.State == FailureReplayPending {
		return nil
	}

	if entry.State == FailureReplayed {
		return ErrAlreadyReplayed
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	result, err := s.db.ExecContext(ctx, `UPDATE public.gof_failed_messages
		SET state = 'pending', replay_attempts = 0, replay_count = replay_count + 1,
		    available_at = clock_timestamp(), lease_token = NULL, last_error = ''
		WHERE id = $1 AND state = 'failed' AND replay_count = $2`, id, entry.ReplayCount)
	if err != nil {
		return fmt.Errorf("pubsub: queue replay: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if count == 1 {
		return nil
	}

	current, err := s.get(ctx, id)
	if err != nil {
		return err
	}

	if current.State == FailureReplayed {
		return ErrAlreadyReplayed
	}

	return nil
}

// DeleteFailed removes only successfully replayed records. Failed and pending
// records must be retained so storage management cannot silently discard work.
func (s *FailedStore) DeleteFailed(ctx context.Context, id string) error {
	if !validFailureID(id) {
		return errors.New("pubsub: invalid failure ID")
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	result, err := s.db.ExecContext(ctx, "DELETE FROM public.gof_failed_messages WHERE id = $1 AND state = 'replayed'", id)
	if err != nil {
		return fmt.Errorf("pubsub: delete replayed failure: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if count == 0 {
		return errors.New("pubsub: record is missing or has not been replayed successfully")
	}

	return nil
}

const failureColumns = `id, topic, consumer_group, message_id, data, raw_data, trace_context,
    reason, last_error, deliveries, failed_at, state, replay_attempts, replay_count,
    replayed_at, COALESCE(lease_token, '')`

type failedEntry struct {
	FailedMessage
	trace map[string]string
	token string
}

type failureScanner interface {
	Scan(...any) error
}

func scanFailure(row failureScanner) (failedEntry, error) {
	var entry failedEntry
	var encoded []byte
	var replayed sql.NullTime
	err := row.Scan(&entry.ID, &entry.Message.Topic, &entry.Group, &entry.Message.ID, &entry.Message.Data,
		&entry.Raw, &encoded, &entry.Reason, &entry.LastError, &entry.Deliveries, &entry.FailedAt,
		&entry.State, &entry.ReplayAttempts, &entry.ReplayCount, &replayed, &entry.token)
	if errors.Is(err, sql.ErrNoRows) {
		return failedEntry{}, ErrFailedNotFound
	}

	if err != nil {
		return failedEntry{}, fmt.Errorf("pubsub: read failure: %w", err)
	}

	if err := json.Unmarshal(encoded, &entry.trace); err != nil {
		return failedEntry{}, fmt.Errorf("pubsub: decode failed-message context: %w", err)
	}

	entry.ReplayedAt = replayed.Time

	return entry, nil
}

func (s *FailedStore) get(ctx context.Context, id string) (failedEntry, error) {
	if !validFailureID(id) {
		return failedEntry{}, errors.New("pubsub: invalid failure ID")
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	return scanFailure(s.db.QueryRowContext(ctx, "SELECT "+failureColumns+" FROM public.gof_failed_messages WHERE id = $1", id))
}

func (s *FailedStore) archive(ctx context.Context, id string, cfg ConsumerConfig, event envelope, raw []byte, deliveries uint64, reason error) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	carrier := make(map[string]string, len(event.Trace))
	for key, value := range event.Trace {
		key = strings.ReplaceAll(strings.ToValidUTF8(key, "�"), "\x00", "")
		carrier[key] = strings.ReplaceAll(strings.ToValidUTF8(value, "�"), "\x00", "")
	}

	encoded, err := json.Marshal(carrier)
	if err != nil {
		return err
	}

	data := event.Data
	if data == nil {
		data = []byte{}
	}

	idValue := event.ID
	var decoded envelope
	if json.Unmarshal(raw, &decoded) != nil || validateID(idValue) != nil {
		idValue = ""
	}

	if raw == nil {
		raw = []byte{}
	}

	_, err = s.db.ExecContext(ctx, `INSERT INTO public.gof_failed_messages
		(id, topic, consumer_group, message_id, data, raw_data, trace_context, reason, deliveries)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9) ON CONFLICT (id) DO NOTHING`,
		id, cfg.Topic, cfg.Group, idValue, data, raw, string(encoded), safeFailureReason(reason), int64(min(deliveries, uint64(1<<63-1))))
	if err != nil {
		return fmt.Errorf("pubsub: store failed message: %w", err)
	}

	return nil
}

func (s *FailedStore) claim(ctx context.Context, cfg ConsumerConfig) (failedEntry, error) {
	token := rand.Text()
	return scanFailure(s.db.QueryRowContext(ctx, `UPDATE public.gof_failed_messages
		SET lease_token = $1, available_at = clock_timestamp() + $2::bigint * interval '1 microsecond',
		    replay_attempts = replay_attempts + 1
		WHERE id = (SELECT id FROM public.gof_failed_messages WHERE topic = $3 AND consumer_group = $4
		    AND state = 'pending' AND available_at <= clock_timestamp()
		    ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING `+failureColumns, token, (2 * s.cfg.ReplayTimeout).Microseconds(), cfg.Topic, cfg.Group))
}

func (s *FailedStore) finishReplay(ctx context.Context, entry failedEntry, state FailureState, delay time.Duration, failure error) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	result, err := s.db.ExecContext(ctx, `UPDATE public.gof_failed_messages SET state = $3,
		available_at = clock_timestamp() + $4::bigint * interval '1 microsecond', lease_token = NULL,
		last_error = $5, replayed_at = CASE WHEN $3 = 'replayed' THEN clock_timestamp() ELSE NULL END
		WHERE id = $1 AND lease_token = $2`, entry.ID, entry.token, state, delay.Microseconds(), safeFailureReason(failure))
	if err != nil {
		return fmt.Errorf("pubsub: finalize replay: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if count != 1 {
		return errors.New("pubsub: replay lease no longer owned")
	}

	return nil
}

func validFailureID(id string) bool {
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == sha256.Size
}

func safeFailureReason(err error) string {
	if err == nil {
		return ""
	}

	text := []rune(strings.ReplaceAll(strings.ToValidUTF8(err.Error(), "�"), "\x00", ""))
	if len(text) > 4096 {
		text = text[:4096]
	}

	return string(text)
}
