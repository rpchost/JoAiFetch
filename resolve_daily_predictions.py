"""
Daily Prediction Resolution Script

Resolves yesterday's global predictions by comparing them with actual market data.
Also resolves linked signals based on actual outcomes.

Run once per day after midnight UTC via GitHub Actions.

Workflow:
1. Fetch unresolved predictions from 2 days ago (for yesterday's actual data)
2. Get actual OHLC data from crypto_candles for yesterday
3. Calculate accuracy score
4. Update prediction with actual values and accuracy
5. Resolve linked signals (hit target, hit stop loss, or expired)
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta, date, timezone
from typing import List, Tuple, Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

COINS = [
    "BTCUSD", "ETHUSD",
    # "SOLUSD", "ADAUSD", "BNBUSD",
    # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    # "LINKUSD", "AVAXUSD", "TONUSD"
]

TIMEFRAMES = ["1 day", "1 hour"]

PRETTY_NAME = {
    "BTCUSD": "Bitcoin",
    "ETHUSD": "Ethereum",
    # "SOLUSD": "Solana",
    # "ADAUSD": "Cardano",
    # "BNBUSD": "Binance Coin",
    # "XRPUSD": "Ripple",
    # "DOGEUSD": "Dogecoin",
    # "SHIBUSD": "Shiba Inu",
    # "PEPEUSD": "Pepe",
    # "LINKUSD": "Chainlink",
    # "AVAXUSD": "Avalanche",
    # "TONUSD": "Toncoin"
}


# ==================== HELPER FUNCTIONS ====================

def get_target_date() -> date:
    """
    Get the date we're resolving for (yesterday).
    Predictions generated 2 days ago should match actual data from yesterday.
    """
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


async def get_unresolved_predictions(
    conn: asyncpg.Connection,
    target_date: date,
    timeframe: str
) -> List[asyncpg.Record]:
    """
    Fetch all unresolved predictions for the target date and timeframe.
    
    Args:
        conn: Database connection
        target_date: Date to resolve predictions for
        timeframe: Timeframe to resolve ('1 day', '1 hour', etc.)
    
    Returns:
        List of prediction records
    """
    query = """
        SELECT 
            id, symbol, timeframe,
            predicted_open, predicted_high, predicted_low, predicted_close,
            for_date, predicted_at
        FROM daily_predictions
        WHERE for_date = $1
          AND timeframe = $2
          AND resolved_at IS NULL
          AND predicted_open IS NOT NULL
        ORDER BY symbol
    """
    
    return await conn.fetch(query, target_date, timeframe)


async def get_actual_candle(
    conn: asyncpg.Connection,
    symbol: str,
    target_date: date,
    timeframe: str
) -> Optional[asyncpg.Record]:
    """
    Fetch the actual OHLC candle for a symbol on a specific date.
    
    Args:
        conn: Database connection
        symbol: Trading pair symbol
        target_date: Date of the candle
        timeframe: Timeframe of the candle
    
    Returns:
        Candle record or None if not found
    """
    # Convert timeframe format: "1 day" -> "1day", "1 hour" -> "1hour"
    db_timeframe = timeframe.replace(" ", "")
    
    # For daily candles, match on date
    if timeframe == "1 day":
        day_start = datetime(
            target_date.year, target_date.month, target_date.day,
            tzinfo=timezone.utc
        )
        day_end = day_start + timedelta(days=1)
        
        query = """
            SELECT open, high, low, close, timestamp
            FROM crypto_candles
            WHERE symbol = $1
              AND timeframe = $2
              AND timestamp >= $3
              AND timestamp < $4
            ORDER BY timestamp DESC
            LIMIT 1
        """
        
        return await conn.fetchrow(query, symbol, db_timeframe, day_start, day_end)
    
    # For hourly candles, we need the last candle of the day
    else:
        day_start = datetime(
            target_date.year, target_date.month, target_date.day,
            tzinfo=timezone.utc
        )
        day_end = day_start + timedelta(days=1)
        
        query = """
            SELECT open, high, low, close, timestamp
            FROM crypto_candles
            WHERE symbol = $1
              AND timeframe = $2
              AND timestamp >= $3
              AND timestamp < $4
            ORDER BY timestamp DESC
            LIMIT 1
        """
        
        return await conn.fetchrow(query, symbol, db_timeframe, day_start, day_end)


def calculate_accuracy(predicted_close: float, actual_close: float) -> float:
    """
    Calculate accuracy score based on prediction error.
    
    Args:
        predicted_close: Predicted closing price
        actual_close: Actual closing price
    
    Returns:
        Accuracy score between 0 and 1
    """
    if actual_close == 0:
        return 0.0
    
    error = abs(predicted_close - actual_close) / actual_close
    accuracy = max(0.0, 1.0 - error)
    
    return round(accuracy, 4)


async def update_prediction(
    conn: asyncpg.Connection,
    prediction_id: int,
    actual_open: float,
    actual_high: float,
    actual_low: float,
    actual_close: float,
    accuracy_score: float
) -> None:
    """
    Update a prediction with actual values and mark it as resolved.
    
    Args:
        conn: Database connection
        prediction_id: ID of the prediction to update
        actual_open: Actual opening price
        actual_high: Actual high price
        actual_low: Actual low price
        actual_close: Actual closing price
        accuracy_score: Calculated accuracy score
    """
    query = """
        UPDATE daily_predictions
        SET 
            actual_open = $1,
            actual_high = $2,
            actual_low = $3,
            actual_close = $4,
            accuracy_score = $5,
            resolved_at = NOW()
        WHERE id = $6
    """
    
    await conn.execute(
        query,
        actual_open, actual_high, actual_low, actual_close,
        accuracy_score, prediction_id
    )


async def resolve_linked_signal(
    conn: asyncpg.Connection,
    prediction_id: int,
    actual_close: float
) -> Optional[str]:
    """
    Resolve the signal linked to a prediction based on actual close price.
    
    Args:
        conn: Database connection
        prediction_id: ID of the prediction
        actual_close: Actual closing price
    
    Returns:
        Signal status or None if no signal found
    """
    # Fetch linked signal
    signal_row = await conn.fetchrow("""
        SELECT id, direction, entry_price, target_price_1, stop_loss, status
        FROM signals
        WHERE daily_prediction_id = $1
        LIMIT 1
    """, prediction_id)
    
    if not signal_row:
        return None
    
    # Skip if already resolved
    if signal_row['status'] in ['hit_target', 'hit_sl', 'expired', 'cancelled', 'closed']:
        return signal_row['status']
    
    # Determine final status
    direction = signal_row['direction']
    target_price = signal_row['target_price_1']
    stop_loss = signal_row['stop_loss']
    
    # Safety check
    if target_price is None or stop_loss is None:
        status = 'expired'
    elif direction == 'LONG':
        if actual_close >= target_price:
            status = 'hit_target'
        elif actual_close <= stop_loss:
            status = 'hit_sl'
        else:
            status = 'closed'
    elif direction == 'SHORT':
        if actual_close <= target_price:
            status = 'hit_target'
        elif actual_close >= stop_loss:
            status = 'hit_sl'
        else:
            status = 'closed'
    else:
        status = 'expired'
    
    # Update signal
    await conn.execute("""
        UPDATE signals
        SET 
            status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, status, signal_row['id'])
    
    return status


async def resolve_predictions_for_timeframe(
    conn: asyncpg.Connection,
    target_date: date,
    timeframe: str
) -> Tuple[int, int]:
    """
    Resolve all predictions for a specific timeframe.
    
    Args:
        conn: Database connection
        target_date: Date to resolve for
        timeframe: Timeframe to resolve
    
    Returns:
        Tuple of (resolved_count, skipped_count)
    """
    predictions = await get_unresolved_predictions(conn, target_date, timeframe)
    
    if not predictions:
        logger.info(f"No unresolved {timeframe} predictions found for {target_date}")
        return 0, 0
    
    logger.info(f"Found {len(predictions)} unresolved {timeframe} predictions for {target_date}")
    
    resolved_count = 0
    skipped_count = 0
    
    for pred in predictions:
        coin_name = PRETTY_NAME.get(pred['symbol'], pred['symbol'])
        
        # Get actual candle
        actual_candle = await get_actual_candle(
            conn,
            pred['symbol'],
            target_date,
            timeframe
        )
        
        if not actual_candle:
            logger.warning(f"  [{coin_name} {timeframe}] No actual candle found - skipping")
            skipped_count += 1
            continue
        
        # Extract actual values
        actual_open = float(actual_candle['open'])
        actual_high = float(actual_candle['high'])
        actual_low = float(actual_candle['low'])
        actual_close = float(actual_candle['close'])
        
        # Calculate accuracy
        pred_close = float(pred['predicted_close'])
        accuracy = calculate_accuracy(pred_close, actual_close)
        
        # Update prediction
        await update_prediction(
            conn,
            pred['id'],
            actual_open, actual_high, actual_low, actual_close,
            accuracy
        )
        
        # Resolve linked signal
        signal_status = await resolve_linked_signal(
            conn,
            pred['id'],
            actual_close
        )
        
        # Determine direction correctness
        pred_open = float(pred['predicted_open'])
        direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
        arrow = "↑" if direction_correct else "↓"
        
        # Log result
        signal_info = f" | Signal: {signal_status}" if signal_status else ""
        logger.info(
            f"  ✓ [{coin_name} {timeframe}] "
            f"Predicted: ${pred_close:,.2f} | Actual: ${actual_close:,.2f} | "
            f"Accuracy: {accuracy:.1%} {arrow}{signal_info}"
        )
        
        resolved_count += 1
    
    return resolved_count, skipped_count


async def main():
    """Main execution function"""
    print("=" * 80)
    print("JOAI DAILY PREDICTIONS RESOLVER")
    print(f"Run time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    target_date = get_target_date()
    logger.info(f"Resolving predictions for {target_date}")
    
    conn = None
    try:
        # Connect to database
        conn = await asyncpg.connect(DATABASE_URL)
        
        total_resolved = 0
        total_skipped = 0
        
        # Resolve each timeframe
        for timeframe in TIMEFRAMES:
            logger.info(f"\n--- Resolving {timeframe} predictions ---")
            
            resolved, skipped = await resolve_predictions_for_timeframe(
                conn,
                target_date,
                timeframe
            )
            
            total_resolved += resolved
            total_skipped += skipped
            
            logger.info(f"Resolved: {resolved} | Skipped: {skipped}")
        
        # Summary
        print("\n" + "=" * 80)
        print(f"SUMMARY for {target_date}")
        print(f"  Total resolved: {total_resolved}")
        print(f"  Total skipped: {total_skipped}")
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        if conn:
            await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
    
# # resolve_daily_predictions.py
# # Daily job: Resolve yesterday's predictions + update linked signals
# # Run once per day after midnight UTC

# import os
# import psycopg2
# from datetime import datetime, timedelta, date
# from dotenv import load_dotenv
# from generate_signals import Signals

# load_dotenv()

# DATABASE_URL = os.getenv("DATABASE_URL")
# if DATABASE_URL and "sslmode" not in DATABASE_URL:
#     DATABASE_URL += "?sslmode=require"

# COINS = [
#     "BTCUSD", "ETHUSD", 
#     # "SOLUSD", "ADAUSD", "BNBUSD",
#     # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
#     # "LINKUSD", "AVAXUSD", "TONUSD"
# ]

# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", 
#     # "SOLUSD": "Solana",
#     # "ADAUSD": "Cardano", "BNBUSD": "Binance Coin", "XRPUSD": "Ripple",
#     # "DOGEUSD": "Dogecoin", "SHIBUSD": "Shiba Inu", "PEPEUSD": "Pepe",
#     # "LINKUSD": "Chainlink", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin"
# }

# def get_prediction_generation_date() -> date:
#     """Date when the prediction was generated (2 days ago from now)"""
#     return (datetime.utcnow() - timedelta(days=2)).date()

# def update_signal_with_resolution(cur, daily_prediction_id: int, accuracy_score: float, 
#                                  direction_correct: bool, actual_close: float, pred_close: float):
#     """
#     Update the linked signal with final accuracy and derived status
#     All values are checked for None to prevent crashes
#     """
#     # Fetch the signal (if exists)
#     cur.execute("""
#         SELECT id, target_price_1, stop_loss, direction
#         FROM signals
#         WHERE daily_prediction_id = %s
#         LIMIT 1
#     """, (daily_prediction_id,))
    
#     signal_row = cur.fetchone()
#     if not signal_row:
#         print("  No linked signal found → skipping update")
#         return

#     signal_id, target_price_1, stop_loss, direction = signal_row

#     # Safety: if target or SL is None → default to expired (no meaningful hit check)
#     if target_price_1 is None or stop_loss is None:
#         status = 'expired'
#         print("  Signal missing target/SL → marked as expired")
#     else:
#         # Derive final status
#         status = 'expired'
#         if direction == 'LONG':
#             if actual_close >= target_price_1:
#                 status = 'hit_target'
#             elif actual_close <= stop_loss:
#                 status = 'hit_sl'
#         elif direction == 'SHORT':
#             if actual_close <= target_price_1:
#                 status = 'hit_target'
#             elif actual_close >= stop_loss:
#                 status = 'hit_sl'

#     # Update signal with final accuracy & status
#     cur.execute("""
#         UPDATE signals
#         SET 
#             accuracy_score = %s,
#             status = %s,
#             updated_at = NOW()
#         WHERE id = %s
#     """, (round(accuracy_score, 4), status, signal_id))

#     print(f" → Signal updated: {status} (Acc: {accuracy_score:.1%})")

# def resolve_predictions():
#     prediction_date = get_prediction_generation_date()
#     target_date = prediction_date + timedelta(days=1)

#     print(f"Resolving predictions generated on {prediction_date} → actual candle on {target_date}")

#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()

#         # Initialize Signals class (it will use the same DB connection)
#         signal_gen = Signals()

#         resolved_count = 0

#         for symbol in COINS:
#             coin_name = PRETTY_NAME.get(symbol, symbol)
#             print(f"  [{coin_name}] ", end="", flush=True)

#             # Step 1: Find pending prediction
#             cur.execute("""
#                 SELECT id, predicted_open, predicted_high, predicted_low, predicted_close
#                 FROM daily_predictions 
#                 WHERE symbol = %s 
#                   AND DATE(predicted_at AT TIME ZONE 'UTC') = %s 
#                   AND resolved_at IS NULL
#             """, (symbol, prediction_date))

#             pred_row = cur.fetchone()
#             if not pred_row:
#                 print("No pending prediction")
#                 continue

#             pred_id, pred_open, pred_high, pred_low, pred_close = pred_row
#             pred_close = float(pred_close)
#             print(f"Found pred (close ${pred_close:,.2f}) ", end="", flush=True)

#             # Step 2: Get actual daily candle
#             cur.execute("""
#                 SELECT open, high, low, close 
#                 FROM crypto_candles 
#                 WHERE symbol = %s 
#                   AND timeframe = '1day' 
#                   AND DATE(timestamp AT TIME ZONE 'UTC') = %s
#                 ORDER BY timestamp DESC 
#                 LIMIT 1
#             """, (symbol, target_date))

#             actual_row = cur.fetchone()
#             if not actual_row:
#                 print("→ Actual candle not found yet")
#                 continue

#             actual_open, actual_high, actual_low, actual_close = [float(x) for x in actual_row]

#             # Step 3: Calculate accuracy
#             if actual_close == 0:
#                 accuracy_score = 0.0
#             else:
#                 error = abs(pred_close - actual_close) / actual_close
#                 accuracy_score = max(0.0, 1.0 - error)

#             accuracy_score = round(accuracy_score, 4)

#             # Step 4: Update prediction
#             cur.execute("""
#                 UPDATE daily_predictions 
#                 SET 
#                     actual_open = %s,
#                     actual_high = %s,
#                     actual_low = %s,
#                     actual_close = %s,
#                     resolved_at = NOW(),
#                     accuracy_score = %s
#                 WHERE id = %s
#             """, (actual_open, actual_high, actual_low, actual_close, accuracy_score, pred_id))

#             # Step 5: Update linked signal (if exists)
#             direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
#             direction_arrow = ' ↑' if direction_correct else ' ↓'

#             result = signal_gen.resolve_signal(pred_id, actual_close)

#             if result['success']:
#                 print(f"→ Signal resolved: {result['status']} | Actual close: ${actual_close:,.2f}")
#             else:
#                 print(f"→ {result['reason']}")

#             print(f"→ Resolved! Actual close: ${actual_close:,.2f} | "
#                   f"Accuracy: {accuracy_score:.1%}{direction_arrow}")

#             # update_signal_with_resolution(cur, pred_id, accuracy_score, direction_correct, 
#             #                              actual_close, pred_close)

#             # print(f"→ Resolved! Actual close: ${actual_close:,.2f} | "
#             #       f"Accuracy: {accuracy_score:.1%}{direction_arrow}")

#             resolved_count += 1

#         conn.commit()
#         print(f"\nSuccessfully resolved {resolved_count}/{len(COINS)} predictions for {target_date}")

#     except Exception as e:
#         print(f"\nDATABASE ERROR: {e}")
#         import traceback
#         traceback.print_exc()
#         if 'conn' in locals():
#             conn.rollback()
#     finally:
#         if 'cur' in locals():
#             cur.close()
#         if 'conn' in locals():
#             conn.close()


# def main():
#     print("=" * 80)
#     print("JOAI DAILY PREDICTIONS + SIGNALS RESOLVER")
#     print(f"Run time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("=" * 80)

#     resolve_predictions()

#     print("=" * 80)
#     print("Resolution complete")
#     print("=" * 80)


# if __name__ == "__main__":
#     main()