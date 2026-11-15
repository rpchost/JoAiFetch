-- Signal outcome tracking (Phase 0).
-- Additive and idempotent: safe to run more than once.
--
-- entry_candle_ts / atr_at_entry : make a signal's pricing reproducible.
-- logic_version                  : 1 = legacy geometry, 2 = current logic.
--                                 The resolver only touches rows >= 2.
-- exit_price / exit_reason /
-- resolved_at / realized_r       : the measured result of the trade.

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS entry_candle_ts timestamptz,
    ADD COLUMN IF NOT EXISTS atr_at_entry   numeric,
    ADD COLUMN IF NOT EXISTS logic_version  integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS exit_price     numeric,
    ADD COLUMN IF NOT EXISTS exit_reason    text,
    ADD COLUMN IF NOT EXISTS realized_r     numeric,
    ADD COLUMN IF NOT EXISTS resolved_at    timestamptz;

-- The resolver scans only open, current-logic signals.
CREATE INDEX IF NOT EXISTS idx_signals_open_v2
    ON public.signals (status, logic_version)
    WHERE status = 'active';
