"""
Personal Daily Prediction Resolution Script

Resolves yesterday's personal predictions by comparing them with actual market data.
Also resolves linked personal signals based on actual outcomes.

Run once per day after midnight UTC via GitHub Actions.

Workflow:
1. Fetch unresolved personal predictions for yesterday
2. Get actual OHLC data from crypto_candles for yesterday
3. Calculate accuracy score and direction correctness
4. Update prediction with actual values and accuracy
5. Resolve linked signals (hit target, hit stop loss, or expired)
"""

import os
import sys
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

def get_target_date(manual_override: Optional[str] = None) -> date:
    """
    Get the date we're resolving for.
    
    Args:
        manual_override: Manual date string in YYYY-MM-DD format
    
    Returns:
        Date to resolve predictions for
    """
    if manual_override:
        try:
            return datetime.strptime(manual_override, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {manual_override}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Default to yesterday
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


async def get_unresolved_predictions(
    conn: asyncpg.Connection,
    target_date: date,
    timeframe: str
) -> List[asyncpg.Record]:
    """
    Fetch all unresolved personal predictions for the target date and timeframe.
    
    Args:
        conn: Database connection
        target_date: Date to resolve predictions for
        timeframe: Timeframe to resolve ('1 day', '1 hour', etc.)
    
    Returns:
        List of prediction records
    """
    query = """
        SELECT 
            id, user_id, symbol, timeframe,
            predicted_open, predicted_high, predicted_low, predicted_close,
            for_date, predicted_at, custom_indicator_id
        FROM personal_daily_predictions
        WHERE for_date = $1
          AND timeframe = $2
          AND resolved_at IS NULL
          AND predicted_open IS NOT NULL
        ORDER BY user_id, symbol
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
    
    # Define time range for the target date
    day_start = datetime(
        target_date.year, target_date.month, target_date.day,
        tzinfo=timezone.utc
    )
    day_end = day_start + timedelta(days=1)
    
    # For daily candles, get the candle for that specific day
    if timeframe == "1 day":
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
    else:
        # For hourly (and other intraday), get the last candle of the day
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


def is_direction_correct(
    predicted_open: float,
    predicted_close: float,
    actual_open: float,
    actual_close: float
) -> bool:
    """
    Check if the predicted direction matches the actual direction.
    
    Args:
        predicted_open: Predicted opening price
        predicted_close: Predicted closing price
        actual_open: Actual opening price
        actual_close: Actual closing price
    
    Returns:
        True if direction matches, False otherwise
    """
    predicted_direction = predicted_close > predicted_open
    actual_direction = actual_close > actual_open
    
    return predicted_direction == actual_direction


async def update_prediction(
    conn: asyncpg.Connection,
    prediction_id: int,
    actual_open: float,
    actual_high: float,
    actual_low: float,
    actual_close: float,
    accuracy_score: float,
    direction_correct: bool
) -> None:
    """
    Update a personal prediction with actual values and mark it as resolved.
    
    Args:
        conn: Database connection
        prediction_id: ID of the prediction to update
        actual_open: Actual opening price
        actual_high: Actual high price
        actual_low: Actual low price
        actual_close: Actual closing price
        accuracy_score: Calculated accuracy score
        direction_correct: Whether direction was correct
    """
    query = """
        UPDATE personal_daily_predictions
        SET 
            actual_open = $1,
            actual_high = $2,
            actual_low = $3,
            actual_close = $4,
            accuracy_score = $5,
            direction_correct = $6,
            resolved_at = NOW()
        WHERE id = $7
    """
    
    await conn.execute(
        query,
        actual_open, actual_high, actual_low, actual_close,
        accuracy_score, direction_correct, prediction_id
    )


async def resolve_linked_signal(
    conn: asyncpg.Connection,
    prediction_id: int,
    actual_close: float
) -> Optional[str]:
    """
    Resolve the signal linked to a personal prediction based on actual close price.
    
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
        WHERE personal_prediction_id = $1
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
) -> Tuple[int, int, int]:
    """
    Resolve all personal predictions for a specific timeframe.
    
    Args:
        conn: Database connection
        target_date: Date to resolve for
        timeframe: Timeframe to resolve
    
    Returns:
        Tuple of (resolved_count, skipped_count, users_count)
    """
    predictions = await get_unresolved_predictions(conn, target_date, timeframe)
    
    if not predictions:
        logger.info(f"No unresolved {timeframe} personal predictions found for {target_date}")
        return 0, 0, 0
    
    unique_users = len(set(pred['user_id'] for pred in predictions))
    logger.info(
        f"Found {len(predictions)} unresolved {timeframe} personal predictions "
        f"for {target_date} ({unique_users} users)"
    )
    
    resolved_count = 0
    skipped_count = 0
    
    for pred in predictions:
        user_id = pred['user_id']
        coin_name = PRETTY_NAME.get(pred['symbol'], pred['symbol'])
        
        # Get actual candle
        actual_candle = await get_actual_candle(
            conn,
            pred['symbol'],
            target_date,
            timeframe
        )
        
        if not actual_candle:
            logger.warning(
                f"  [User {user_id}] [{coin_name} {timeframe}] "
                f"No actual candle found - skipping"
            )
            skipped_count += 1
            continue
        
        # Extract actual values
        actual_open = float(actual_candle['open'])
        actual_high = float(actual_candle['high'])
        actual_low = float(actual_candle['low'])
        actual_close = float(actual_candle['close'])
        
        # Extract predicted values
        pred_open = float(pred['predicted_open'] or 0)
        pred_close = float(pred['predicted_close'] or 0)
        
        # Calculate accuracy
        accuracy = calculate_accuracy(pred_close, actual_close)
        
        # Check direction correctness
        direction_correct = is_direction_correct(
            pred_open, pred_close,
            actual_open, actual_close
        )
        
        # Update prediction
        await update_prediction(
            conn,
            pred['id'],
            actual_open, actual_high, actual_low, actual_close,
            accuracy, direction_correct
        )
        
        # Resolve linked signal
        signal_status = await resolve_linked_signal(
            conn,
            pred['id'],
            actual_close
        )
        
        # Log result
        arrow = "↑" if direction_correct else "↓"
        signal_info = f" | Signal: {signal_status}" if signal_status else ""
        
        logger.info(
            f"  ✓ [User {user_id}] [{coin_name} {timeframe}] "
            f"Predicted: ${pred_close:,.2f} | Actual: ${actual_close:,.2f} | "
            f"Accuracy: {accuracy:.1%} {arrow}{signal_info}"
        )
        
        resolved_count += 1
    
    return resolved_count, skipped_count, unique_users


async def main():
    """Main execution function"""
    print("=" * 80)
    print("JOAI PERSONAL PREDICTIONS RESOLVER")
    print(f"Run time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Check for manual date override
    manual_date = sys.argv[1] if len(sys.argv) > 1 else None
    target_date = get_target_date(manual_date)
    
    if manual_date:
        logger.info(f"Manual override: Resolving for {target_date}")
    else:
        logger.info(f"Auto mode: Resolving predictions for {target_date}")
    
    conn = None
    try:
        # Connect to database
        conn = await asyncpg.connect(DATABASE_URL)
        
        total_resolved = 0
        total_skipped = 0
        total_users = set()
        
        # Resolve each timeframe
        for timeframe in TIMEFRAMES:
            logger.info(f"\n--- Resolving {timeframe} personal predictions ---")
            
            resolved, skipped, users = await resolve_predictions_for_timeframe(
                conn,
                target_date,
                timeframe
            )
            
            total_resolved += resolved
            total_skipped += skipped
            total_users.add(users)
            
            logger.info(f"Resolved: {resolved} | Skipped: {skipped} | Users: {users}")
        
        # Summary
        print("\n" + "=" * 80)
        print(f"SUMMARY for {target_date}")
        print(f"  Total resolved: {total_resolved}")
        print(f"  Total skipped: {total_skipped}")
        print(f"  Unique users: {max(total_users) if total_users else 0}")
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
    
# # resolve_personal_daily_predictions.py
# # Daily job: Resolve PERSONAL predictions + update linked signals
# # Run once per day after midnight UTC

# import os
# import sys
# import psycopg2
# from datetime import datetime, timedelta, date, timezone
# from dotenv import load_dotenv

# # Import Signals class from generate_signals.py
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

# def resolve_personal_predictions(target_for_date=None):
#     """
#     Resolve personal predictions for the given for_date + update linked signals.
#     If None, defaults to yesterday.
#     """
#     if target_for_date is None:
#         target_for_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
#         print(f"Auto mode: Resolving predictions for yesterday → for_date = {target_for_date}")
#     else:
#         print(f"Manual mode: Resolving predictions for for_date = {target_for_date}")

#     conn = None
#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()
        
#         # Initialize Signals class (shared DB connection)
#         signal_gen = Signals()

#         resolved_count = 0

#         # Fetch unresolved personal predictions
#         cur.execute("""
#             SELECT 
#                 id, user_id, symbol, 
#                 predicted_open, predicted_high, predicted_low, predicted_close
#             FROM personal_daily_predictions
#             WHERE for_date = %s
#               AND resolved_at IS NULL
#               AND predicted_open IS NOT NULL
#         """, (target_for_date,))

#         rows = cur.fetchall()
#         if not rows:
#             print(f"No unresolved personal predictions found for for_date = {target_for_date}")
#             return

#         print(f"Found {len(rows)} personal predictions to resolve")

#         # Timestamp range for the actual daily candle
#         day_start = datetime(target_for_date.year, target_for_date.month, target_for_date.day, tzinfo=timezone.utc)
#         day_end = day_start + timedelta(days=1)

#         for row in rows:
#             pred_id, user_id, symbol, pred_open, pred_high, pred_low, pred_close = row

#             # Safe float conversion
#             pred_open = float(pred_open) if pred_open is not None else 0.0
#             pred_high = float(pred_high) if pred_high is not None else pred_open
#             pred_low = float(pred_low) if pred_low is not None else pred_open
#             pred_close = float(pred_close) if pred_close is not None else pred_open

#             coin_name = PRETTY_NAME.get(symbol, symbol)
#             print(f"  User {user_id} [{coin_name}] ", end="", flush=True)

#             # Step 2: Get actual daily candle
#             cur.execute("""
#                 SELECT open, high, low, close
#                 FROM crypto_candles
#                 WHERE symbol = %s 
#                   AND timeframe = '1day'
#                   AND timestamp >= %s
#                   AND timestamp < %s
#                 ORDER BY timestamp DESC 
#                 LIMIT 1
#             """, (symbol, day_start, day_end))

#             actual_row = cur.fetchone()
#             if not actual_row:
#                 print("→ Actual daily candle missing")
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
#                 UPDATE personal_daily_predictions
#                 SET 
#                     actual_open = %s,
#                     actual_high = %s,
#                     actual_low = %s,
#                     actual_close = %s,
#                     resolved_at = NOW(),
#                     accuracy_score = %s,
#                     direction_correct = %s
#                 WHERE id = %s
#             """, (
#                 actual_open, actual_high, actual_low, actual_close,
#                 accuracy_score,
#                 (pred_close > pred_open) == (actual_close > actual_open),
#                 pred_id
#             ))

#             # Step 5: Resolve linked signal using Signals.resolve
#             direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
#             arrow = " ↑" if direction_correct else " ↓"

#             result = signal_gen.resolve(pred_id, actual_close)
            
#             if result['success']:
#                 print(f"→ Signal resolved: {result['status']} | Actual close: ${actual_close:,.2f}")
#             else:
#                 print(f"→ {result['reason']}")

#             print(f"→ Resolved! Actual close: ${actual_close:,.2f} | "
#                   f"Accuracy: {accuracy_score:.1%}{arrow}")

#             resolved_count += 1

#         conn.commit()
#         print(f"\nSuccessfully resolved {resolved_count} personal predictions for {target_for_date}")

#     except Exception as e:
#         print(f"\nERROR: {e}")
#         import traceback
#         traceback.print_exc()
#         if conn:
#             conn.rollback()
#     finally:
#         if conn:
#             conn.close()

# def main():
#     print("=" * 80)
#     print("JOAI PERSONAL PREDICTIONS + SIGNALS RESOLVER")
#     print(f"Run time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
#     print("=" * 80)

#     target_for_date = None
#     if len(sys.argv) > 1:
#         try:
#             target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
#             print(f"Manual override: Resolving for_date = {target_for_date}")
#         except ValueError:
#             print("Invalid date format. Use YYYY-MM-DD")
#             sys.exit(1)
#     else:
#         print("Auto mode: Resolving yesterday's personal predictions")

#     resolve_personal_predictions(target_for_date)

#     print("=" * 80)
#     print("Personal resolution complete")
#     print("=" * 80)


# if __name__ == "__main__":
#     main()
