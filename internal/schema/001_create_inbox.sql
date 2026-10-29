CREATE TABLE public.gof_inbox (
    consumer text COLLATE "C" NOT NULL CHECK (octet_length(consumer) BETWEEN 1 AND 512),
    message_id text COLLATE "C" NOT NULL CHECK (octet_length(message_id) BETWEEN 1 AND 256),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer, message_id)
);
