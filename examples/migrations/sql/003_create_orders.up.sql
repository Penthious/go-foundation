CREATE TABLE public.orders (
    id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
