# resolve_personal_daily_predictions.py
# Daily job: Resolve PERSONAL predictions + update linked signals
# Supports optional date override via command line (YYYY-MM-DD)

import os
import sys
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

COINS = [
    "BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "BNBUSD",
    "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    "LINKUSD", "AVAXUSD", "TONUSD"
]

PRETTY_NAME = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "SOLUSD": "Solana",
    "ADAUSD": "Cardano", "BNBUSD": "Binance Coin", "XRPUSD": "Ripple",
    "DOGEUSD": "Dogecoin", "SHIBUSD": "Shiba Inu", "PEPEUSD": "Pepe",
    "LINKUSD": "Chainlink", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin"
}

def update_signal_with_resolution(cur, personal_prediction_id: int, accuracy_score: float, 
                                 direction_correct: bool, actual_close: float, pred_close: float):
    """
    Update the linked personal signal with final accuracy and derived status.
    Handles missing target_price_1 / stop_loss gracefully.
    """
    # Fetch the signal (if exists)
    cur.execute("""
        SELECT id, target_price_1, stop_loss, direction
        FROM signals
        WHERE personal_prediction_id = %s
        LIMIT 1
    """, (personal_prediction_id,))
    
    signal_row = cur.fetchone()
    if not signal_row:
        print("  No linked signal found → skipping update")
        return

    signal_id, target_price_1, stop_loss, direction = signal_row

    # Safety: if target or SL is missing → default to expired (no crash)
    if target_price_1 is None or stop_loss is None:
        status = 'expired'
        print(f"  Signal missing target/SL → marked as expired")
    else:
        # Derive final status (only if values exist)
        status = 'expired'
        if direction == 'LONG':
            if actual_close >= target_price_1:
                status = 'hit_target'
            elif actual_close <= stop_loss:
                status = 'hit_sl'
        elif direction == 'SHORT':
            if actual_close <= target_price_1:
                status = 'hit_target'
            elif actual_close >= stop_loss:
                status = 'hit_sl'

    # Update signal (safe even if target/SL were NULL)
    cur.execute("""
        UPDATE signals
        SET 
            accuracy_score = %s,
            status = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (round(accuracy_score, 4), status, signal_id))

    print(f" → Signal updated: {status} (Acc: {accuracy_score:.1%})")

def resolve_personal_predictions(target_for_date=None):
    """
    Resolve personal predictions for the given for_date + update linked signals.
    If None, defaults to yesterday.
    """
    if target_for_date is None:
        target_for_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        print(f"Auto mode: Resolving predictions for yesterday → for_date = {target_for_date}")
    else:
        print(f"Manual mode: Resolving predictions for for_date = {target_for_date}")

    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Fetch unresolved predictions
        cur.execute("""
            SELECT 
                id, user_id, symbol, 
                predicted_open, predicted_high, predicted_low, predicted_close
            FROM personal_daily_predictions
            WHERE for_date = %s
              AND resolved_at IS NULL
              AND predicted_open IS NOT NULL
        """, (target_for_date,))

        rows = cur.fetchall()
        if not rows:
            print(f"No unresolved personal predictions found for for_date = {target_for_date}")
            return

        print(f"Found {len(rows)} predictions to resolve")

        resolved_count = 0

        # Timestamp range for the actual daily candle
        day_start = datetime(target_for_date.year, target_for_date.month, target_for_date.day, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)

        for row in rows:
            pred_id, user_id, symbol, pred_open, pred_high, pred_low, pred_close = row

            # Safe float conversion
            pred_open = float(pred_open) if pred_open is not None else 0.0
            pred_high = float(pred_high) if pred_high is not None else pred_open
            pred_low = float(pred_low) if pred_low is not None else pred_open
            pred_close = float(pred_close) if pred_close is not None else pred_open

            coin_name = PRETTY_NAME.get(symbol, symbol)
            print(f"  User {user_id} [{coin_name}] ", end="", flush=True)

            # Step 2: Get actual daily candle
            cur.execute("""
                SELECT open, high, low, close
                FROM crypto_candles
                WHERE symbol = %s 
                  AND timeframe = '1day'
                  AND timestamp >= %s
                  AND timestamp < %s
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (symbol, day_start, day_end))

            actual_row = cur.fetchone()
            if not actual_row:
                print("→ Actual daily candle missing")
                continue

            actual_open, actual_high, actual_low, actual_close = [float(x) for x in actual_row]

            # Step 3: Calculate accuracy
            if actual_close == 0:
                accuracy_score = 0.0
            else:
                error = abs(pred_close - actual_close) / actual_close
                accuracy_score = max(0.0, 1.0 - error)

            accuracy_score = round(accuracy_score, 4)

            # Step 4: Update prediction
            cur.execute("""
                UPDATE personal_daily_predictions
                SET 
                    actual_open = %s,
                    actual_high = %s,
                    actual_low = %s,
                    actual_close = %s,
                    resolved_at = NOW(),
                    accuracy_score = %s,
                    direction_correct = %s
                WHERE id = %s
            """, (
                actual_open, actual_high, actual_low, actual_close,
                accuracy_score,
                (pred_close > pred_open) == (actual_close > actual_open),
                pred_id
            ))

            # Step 5: Update linked signal (if exists)
            direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
            arrow = " ↑" if direction_correct else " ↓"

            update_signal_with_resolution(cur, pred_id, accuracy_score, direction_correct, 
                                         actual_close, pred_close)

            print(f"→ Resolved! Actual close: ${actual_close:,.2f} | "
                  f"Accuracy: {accuracy_score:.1%}{arrow}")

            resolved_count += 1

        conn.commit()
        print(f"\nSuccessfully resolved {resolved_count} personal predictions for {target_for_date}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    print("=" * 80)
    print("JOAI PERSONAL PREDICTIONS + SIGNALS RESOLVER")
    print(f"Run time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    target_for_date = None
    if len(sys.argv) > 1:
        try:
            target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
            print(f"Manual override: Resolving for_date = {target_for_date}")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        print("Auto mode: Resolving yesterday's predictions")

    resolve_personal_predictions(target_for_date)

    print("=" * 80)
    print("Personal resolution complete")
    print("=" * 80)


if __name__ == "__main__":
    main()

# # resolve_personal_daily_predictions.py
# # Daily job: Resolve PERSONAL predictions with actual data
# # Now supports optional date override to resolve a specific for_date
# # resolve_personal_daily_predictions.py
# # Resolve PERSONAL predictions for a specific for_date (or yesterday by default)

# import os
# import sys
# import psycopg2
# from datetime import datetime, timedelta, timezone
# from dotenv import load_dotenv

# load_dotenv()

# DATABASE_URL = os.getenv("DATABASE_URL")
# if DATABASE_URL and "sslmode" not in DATABASE_URL:
#     DATABASE_URL += "?sslmode=require"

# COINS = [
#     "BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "BNBUSD",
#     "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
#     "LINKUSD", "AVAXUSD", "TONUSD"
# ]

# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "SOLUSD": "Solana",
#     "ADAUSD": "Cardano", "BNBUSD": "Binance Coin", "XRPUSD": "Ripple",
#     "DOGEUSD": "Dogecoin", "SHIBUSD": "Shiba Inu", "PEPEUSD": "Pepe",
#     "LINKUSD": "Chainlink", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin"
# }

# def resolve_personal_predictions(target_for_date=None):
#     """
#     Resolve personal predictions for the given for_date.
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

#         # Fetch unresolved predictions for the target for_date
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

#         print(f"Found {len(rows)} predictions to resolve")

#         resolved_count = 0

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

#             # Fetch actual daily candle using reliable timestamp range
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

#             # Accuracy score
#             if actual_close == 0:
#                 accuracy_score = 0.0
#             else:
#                 error = abs(pred_close - actual_close) / actual_close
#                 accuracy_score = max(0.0, 1.0 - error)

#             # Direction correct
#             predicted_up = pred_close > pred_open
#             actual_up = actual_close > actual_open
#             direction_correct = predicted_up == actual_up

#             arrow = " ↑" if direction_correct else " ↓"
#             print(f"→ Resolved! Acc: {accuracy_score:.1%}{arrow} (Actual close: ${actual_close:,.2f})")

#             # Save actuals and metrics
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
#                 round(accuracy_score, 4),
#                 direction_correct,
#                 pred_id
#             ))

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
#     print("JOAI PERSONAL PREDICTIONS RESOLVER")
#     print(f"Run time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
#     print("=" * 80)

#     target_for_date = None
#     if len(sys.argv) > 1:
#         try:
#             target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
#             print(f"Manual override: Resolving predictions for for_date = {target_for_date}")
#         except ValueError:
#             print("Invalid date format. Use YYYY-MM-DD")
#             sys.exit(1)
#     else:
#         print("Auto mode: Resolving yesterday's predictions")

#     resolve_personal_predictions(target_for_date)

#     print("=" * 80)
#     print("Personal resolution complete")
#     print("=" * 80)

# if __name__ == "__main__":
#     main()