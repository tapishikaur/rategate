-- The token buckets every gateway shares: one row per API key.
--
-- UNLOGGED: changes skip the write-ahead log, so an update is cheaper and a commit doesn't
-- wait for a disk flush. The price: after a database crash the table comes back empty, and
-- every bucket starts full again. For rate-limit state that's acceptable (one extra burst per
-- key), and it's what makes one update per request (exact mode) affordable at all.
CREATE UNLOGGED TABLE IF NOT EXISTS buckets (
  key        text PRIMARY KEY,
  rate       double precision NOT NULL, -- tokens per second, from the gateways' config
  burst      double precision NOT NULL, -- capacity
  tokens     double precision NOT NULL, -- as of updated_at; negative = debt, paid back by refill
  taken      double precision NOT NULL, -- tokens ever taken from this bucket, by all gateways
  updated_at timestamptz      NOT NULL
);

-- A bucket's count now: what it had, plus rate × the time since its last update, capped at
-- the burst. now() is the database's clock, read once at the start of the transaction, so
-- gateways whose own clocks disagree still refill each bucket exactly once.
--
-- GREATEST(0, ...) matters: an update that started before a concurrent one committed resumes
-- with that newer row, whose updated_at is later than this transaction's now(). Without it the
-- elapsed time would be negative and the refill would remove tokens. (The matching
-- GREATEST(updated_at, now()) in the updates keeps updated_at from moving backwards.)
CREATE OR REPLACE FUNCTION rategate_refill(tokens double precision, updated_at timestamptz,
                                           rate double precision, burst double precision)
RETURNS double precision
LANGUAGE sql STABLE
AS $$
  SELECT LEAST(burst, tokens + rate * GREATEST(0, EXTRACT(epoch FROM now() - updated_at)::double precision))
$$;
