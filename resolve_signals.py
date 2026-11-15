# resolve_signals.py - close open signals against the candles that followed them
#
# Runs right after signal generation in both workflows. For every open signal
# written by the current logic it walks the 1-hour candles after the entry and
# records the first level touched (target or stop), or expires the signal at its
# deadline and marks it to market. The evaluation itself lives in
# signal_outcomes.py and is database-free; this file only loads candles and
# writes results.

"""Resolve open signals into measured outcomes."""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from dotenv import load_dotenv

from signal_outcomes import (
    LOGIC_VERSION,
    SignalOutcome,
    evaluate_position,
    realized_r,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CANDLE_TIMEFRAME = "1hour"

# Fallback horizon for a signal whose time_expiry was never written. The
# generator always writes it; this only keeps the resolver self-sufficient.
DEFAULT_EXPIRY_HOURS = 4.0

ACTIVE_QUERY = """
    SELECT
        id,
        symbol::text    AS symbol,
        direction::text AS direction,
        entry_price,
        target_price_1,
        stop_loss,
        entry_candle_ts,
        time_generated,
        time_expiry
    FROM signals
    WHERE status = 'active'
      AND logic_version >= $1
      AND deleted_at IS NULL
    ORDER BY id
"""

CANDLE_QUERY = """
    SELECT timestamp, open, high, low, close
    FROM crypto_candles
    WHERE symbol = $1
      AND timeframe = $2
      AND timestamp > $3
      AND timestamp <= $4
    ORDER BY timestamp
"""

UPDATE_QUERY = """
    UPDATE signals
    SET status       = $1,
        exit_price   = $2,
        exit_reason  = $3,
        resolved_at  = $4,
        realized_r   = $5,
        updated_at   = NOW()
    WHERE id = $6
"""


class SignalResolver:
    """Resolve open signals from stored 1-hour candles."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        expiry_hours: float = DEFAULT_EXPIRY_HOURS,
    ):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables")
        if "sslmode" not in self.database_url:
            self.database_url += "?sslmode=require"
        self.expiry_hours = expiry_hours
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                self.database_url, min_size=1, max_size=4, command_timeout=60
            )
            logger.info("Database connection pool initialized")

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ==================== WORK ====================

    async def resolve_all(self) -> Dict[str, Any]:
        """Resolve every open signal that can be decided now."""
        stats: Dict[str, Any] = {
            "checked": 0,
            "resolved": 0,
            "still_open": 0,
            "errors": 0,
            "by_reason": {},
            "details": [],
        }
        now = datetime.now(timezone.utc)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(ACTIVE_QUERY, LOGIC_VERSION)

        logger.info(f"Found {len(rows)} open signal(s) to check")

        for row in rows:
            stats["checked"] += 1
            try:
                async with self.pool.acquire() as conn:
                    outcome = await self._resolve_one(conn, row, now)
            except Exception as exc:  # one bad row must not stop the rest
                stats["errors"] += 1
                logger.error(f"Signal #{row['id']} could not be resolved: {exc}")
                stats["details"].append(
                    {"signal_id": row["id"], "status": "error", "error": str(exc)}
                )
                continue

            if outcome is None:
                stats["still_open"] += 1
                stats["details"].append(
                    {"signal_id": row["id"], "symbol": row["symbol"], "status": "open"}
                )
                continue

            stats["resolved"] += 1
            reason = outcome.exit_reason
            stats["by_reason"][reason] = stats["by_reason"].get(reason, 0) + 1
            stats["details"].append(
                {
                    "signal_id": row["id"],
                    "symbol": row["symbol"],
                    "status": outcome.status,
                    "reason": reason,
                    "exit_price": outcome.exit_price,
                }
            )

        logger.info(
            f"Signal resolution complete: {stats['resolved']} resolved, "
            f"{stats['still_open']} still open, {stats['errors']} error(s) "
            f"from {stats['checked']} checked"
        )
        return stats

    async def _resolve_one(
        self,
        conn: asyncpg.Connection,
        row: asyncpg.Record,
        now: datetime,
    ) -> Optional[SignalOutcome]:
        """Load the signal's candles, evaluate, and persist the outcome."""
        entry_candle_ts = row["entry_candle_ts"] or row["time_generated"]
        expires_at = row["time_expiry"] or (
            row["time_generated"] + timedelta(hours=self.expiry_hours)
        )

        candles: List[Tuple[Any, ...]] = await conn.fetch(
            CANDLE_QUERY,
            row["symbol"],
            CANDLE_TIMEFRAME,
            entry_candle_ts,
            expires_at,
        )

        outcome = evaluate_position(
            direction=row["direction"],
            entry_price=float(row["entry_price"]),
            target_price=float(row["target_price_1"])
            if row["target_price_1"] is not None
            else None,
            stop_loss=float(row["stop_loss"]) if row["stop_loss"] is not None else None,
            entry_candle_ts=entry_candle_ts,
            expires_at=expires_at,
            candles=[tuple(c) for c in candles],
            now=now,
        )

        if outcome is None:
            logger.debug(f"Signal #{row['id']} still open (expires {expires_at:%Y-%m-%d %H:%M} UTC)")
            return None

        r_multiple = realized_r(
            row["direction"],
            float(row["entry_price"]),
            float(row["stop_loss"]),
            outcome.exit_price,
        )

        await conn.execute(
            UPDATE_QUERY,
            outcome.status,
            outcome.exit_price,
            outcome.exit_reason,
            outcome.resolved_at,
            r_multiple,
            row["id"],
        )

        r_text = f"{r_multiple:+.2f}R" if r_multiple is not None else "n/a"
        logger.info(
            f"Signal #{row['id']} {row['symbol']} {row['direction']}: "
            f"{outcome.status} ({outcome.exit_reason}) at ${outcome.exit_price:.2f} {r_text}"
        )
        return outcome


async def main() -> None:
    print("=" * 80)
    print("JOAI SIGNAL RESOLVER")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)

    async with SignalResolver() as resolver:
        stats = await resolver.resolve_all()

    print("\nResults:")
    print(f"  Checked:     {stats['checked']}")
    print(f"  Resolved:    {stats['resolved']}")
    print(f"  Still open:  {stats['still_open']}")
    print(f"  Errors:      {stats['errors']}")
    if stats["by_reason"]:
        print("  By reason:")
        for reason, count in sorted(stats["by_reason"].items()):
            print(f"    {reason}: {count}")

    print("\n" + "=" * 80)
    print("Signal resolution complete")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
