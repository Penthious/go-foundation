CREATE TABLE public.gof_schedules (
    namespace text COLLATE "C" NOT NULL,
    name text COLLATE "C" NOT NULL,
    schedule text NOT NULL,
    timezone text NOT NULL,
    timeout_ns bigint NOT NULL CHECK (timeout_ns > 0),
    next_run_at timestamptz NOT NULL,
    lease_until timestamptz,
    lease_token text,
    PRIMARY KEY (namespace, name),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
