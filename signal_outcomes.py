"""Signal outcome logic: pure functions, no database access.

A signal is created from a 1-hour candle close and has no deadline. Its result is
decided by walking the 1-hour candles that follow, in order: whichever level
price touches first wins. A signal that touches neither level stays open and is
re-checked on the next run. Passing ``expires_at`` still enables the old
fixed-horizon behaviour for callers that want it.

Ambiguity rule: when a single 1-hour candle reaches both the target and the stop,
the order is unknowable from 1-hour data. Such a candle is never booked as a win
- it is recorded with ``exit_reason='ambiguous'`` at the stop price.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence

# Rows created by the current generation logic carry this version. Older rows
# are ignored by the resolver: their geometry (a target a fraction of one ATR
# away, a stop 7% away) makes them incomparable with new rows.
LOGIC_VERSION = 2

LONG = "LONG"
SHORT = "SHORT"

# Statuses written by this module (all exist in status_enum).
HIT_TARGET = "hit_target"
HIT_SL = "hit_sl"
EXPIRED = "expired"
CLOSED = "closed"

# Candle tuple layout: (timestamp, open, high, low, close)
Candle = Sequence[Any]


@dataclass(frozen=True)
class SignalOutcome:
    """Final state of a signal."""

    status: str
    exit_price: float
    exit_reason: str
    resolved_at: datetime


def realized_r(
    direction: str,
    entry_price: float,
    stop_loss: float,
    exit_price: float,
) -> Optional[float]:
    """
    Result in R multiples: +1.0 when price moved exactly one stop distance in
    the trade's favour, -1.0 at the stop, 0.0 when it ended where it started.
    """
    risk = abs(float(entry_price) - float(stop_loss))
    if risk <= 0:
        return None
    sign = 1.0 if direction == LONG else -1.0
    return sign * (float(exit_price) - float(entry_price)) / risk


def evaluate_position(
    direction: str,
    entry_price: float,
    target_price: Optional[float],
    stop_loss: Optional[float],
    entry_candle_ts: Optional[datetime],
    expires_at: Optional[datetime],
    candles: Iterable[Candle],
    now: datetime,
) -> Optional[SignalOutcome]:
    """
    Walk forward through ``candles`` (ascending, after ``entry_candle_ts``) and
    decide the signal's outcome.

    Returns ``None`` while the signal is still open: no level touched and the
    deadline not reached yet.
    """
    if target_price is None or stop_loss is None:
        return SignalOutcome(
            status=EXPIRED,
            exit_price=float(entry_price),
            exit_reason="invalid_data",
            resolved_at=expires_at or now,
        )

    entry = float(entry_price)
    target = float(target_price)
    stop = float(stop_loss)
    last_close: Optional[float] = None

    for candle in candles:
        timestamp = candle[0]
        open_price = float(candle[1])
        high = float(candle[2])
        low = float(candle[3])
        close = float(candle[4])

        if entry_candle_ts is not None and timestamp <= entry_candle_ts:
            continue
        if expires_at is not None and timestamp > expires_at:
            break

        last_close = close

        if direction == LONG:
            target_gapped = open_price >= target
            stop_gapped = open_price <= stop
            target_touched = target_gapped or high >= target
            stop_touched = stop_gapped or low <= stop
        else:
            target_gapped = open_price <= target
            stop_gapped = open_price >= stop
            target_touched = target_gapped or low <= target
            stop_touched = stop_gapped or high >= stop

        if target_touched and stop_touched:
            # Same candle touched both levels: order unknowable, assume the stop.
            return SignalOutcome(
                status=CLOSED,
                exit_price=open_price if stop_gapped else stop,
                exit_reason="ambiguous",
                resolved_at=timestamp,
            )
        if target_touched:
            return SignalOutcome(
                status=HIT_TARGET,
                exit_price=open_price if target_gapped else target,
                exit_reason="target",
                resolved_at=timestamp,
            )
        if stop_touched:
            return SignalOutcome(
                status=HIT_SL,
                exit_price=open_price if stop_gapped else stop,
                exit_reason="stop",
                resolved_at=timestamp,
            )

    # Nothing touched. With no deadline the signal stays open; with one it expires
    # once that deadline has passed.
    if expires_at is None or now < expires_at:
        return None

    return SignalOutcome(
        status=EXPIRED,
        exit_price=last_close if last_close is not None else entry,
        exit_reason="expired" if last_close is not None else "no_data",
        resolved_at=expires_at or now,
    )
