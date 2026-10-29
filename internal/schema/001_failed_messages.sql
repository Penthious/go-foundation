CREATE TABLE public.gof_failed_messages (
    id text COLLATE "C" PRIMARY KEY,
    topic text COLLATE "C" NOT NULL,
    consumer_group text COLLATE "C" NOT NULL,
    message_id text COLLATE "C" NOT NULL,
    data bytea NOT NULL,
    raw_data bytea NOT NULL,
    trace_context jsonb NOT NULL CHECK (jsonb_typeof(trace_context) = 'object'),
    reason text NOT NULL,
    last_error text NOT NULL DEFAULT '',
    deliveries bigint NOT NULL CHECK (deliveries >= 0),
    failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    state text NOT NULL DEFAULT 'failed' CHECK (state IN ('failed', 'pending', 'replayed')),
    replay_attempts bigint NOT NULL DEFAULT 0 CHECK (replay_attempts >= 0),
    replay_count bigint NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    replayed_at timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token text
);

CREATE INDEX gof_failed_messages_replay_idx
    ON public.gof_failed_messages (topic, consumer_group, available_at, id)
    WHERE state = 'pending';

CREATE INDEX gof_failed_messages_list_idx
    ON public.gof_failed_messages (topic, consumer_group, id);
