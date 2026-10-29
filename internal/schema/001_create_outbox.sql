CREATE TABLE public.gof_outbox (
    id text PRIMARY KEY CHECK (octet_length(id) BETWEEN 1 AND 256),
    topic text NOT NULL,
    data bytea NOT NULL,
    trace_context jsonb NOT NULL CHECK (jsonb_typeof(trace_context) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts bigint NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token text
);

CREATE INDEX gof_outbox_available_idx
    ON public.gof_outbox (available_at, created_at, id);
