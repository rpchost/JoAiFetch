# resolve_daily_predictions.py
# Daily job: Resolve yesterday's predictions + update linked signals
# Run once per day after midnight UTC

import os
import psycopg2
from datetime import datetime, timedelta, date
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

def get_prediction_generation_date() -> date:
    """Date when the prediction was generated (2 days ago from now)"""
    return (datetime.utcnow() - timedelta(days=2)).date()

def update_signal_with_resolution(cur, daily_prediction_id: int, accuracy_score: float, 
                                 direction_correct: bool, actual_close: float, pred_close: float):
    """
    Update the linked signal with final accuracy and derived status
    All values are checked for None to prevent crashes
    """
    # Fetch the signal (if exists)
    cur.execute("""
        SELECT id, target_price_1, stop_loss, direction
        FROM signals
        WHERE daily_prediction_id = %s
        LIMIT 1
    """, (daily_prediction_id,))
    
    signal_row = cur.fetchone()
    if not signal_row:
        print("  No linked signal found → skipping update")
        return

    signal_id, target_price_1, stop_loss, direction = signal_row

    # Safety: if target or SL is None → default to expired (no meaningful hit check)
    if target_price_1 is None or stop_loss is None:
        status = 'expired'
        print("  Signal missing target/SL → marked as expired")
    else:
        # Derive final status
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

    # Update signal with final accuracy & status
    cur.execute("""
        UPDATE signals
        SET 
            accuracy_score = %s,
            status = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (round(accuracy_score, 4), status, signal_id))

    print(f" → Signal updated: {status} (Acc: {accuracy_score:.1%})")

def resolve_predictions():
    prediction_date = get_prediction_generation_date()
    target_date = prediction_date + timedelta(days=1)

    print(f"Resolving predictions generated on {prediction_date} → actual candle on {target_date}")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        resolved_count = 0

        for symbol in COINS:
            coin_name = PRETTY_NAME.get(symbol, symbol)
            print(f"  [{coin_name}] ", end="", flush=True)

            # Step 1: Find pending prediction
            cur.execute("""
                SELECT id, predicted_open, predicted_high, predicted_low, predicted_close
                FROM daily_predictions 
                WHERE symbol = %s 
                  AND DATE(predicted_at AT TIME ZONE 'UTC') = %s 
                  AND resolved_at IS NULL
            """, (symbol, prediction_date))

            pred_row = cur.fetchone()
            if not pred_row:
                print("No pending prediction")
                continue

            pred_id, pred_open, pred_high, pred_low, pred_close = pred_row
            pred_close = float(pred_close)
            print(f"Found pred (close ${pred_close:,.2f}) ", end="", flush=True)

            # Step 2: Get actual daily candle
            cur.execute("""
                SELECT open, high, low, close 
                FROM crypto_candles 
                WHERE symbol = %s 
                  AND timeframe = '1day' 
                  AND DATE(timestamp AT TIME ZONE 'UTC') = %s
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (symbol, target_date))

            actual_row = cur.fetchone()
            if not actual_row:
                print("→ Actual candle not found yet")
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
                UPDATE daily_predictions 
                SET 
                    actual_open = %s,
                    actual_high = %s,
                    actual_low = %s,
                    actual_close = %s,
                    resolved_at = NOW(),
                    accuracy_score = %s
                WHERE id = %s
            """, (actual_open, actual_high, actual_low, actual_close, accuracy_score, pred_id))

            # Step 5: Update linked signal (if exists)
            direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
            direction_arrow = ' ↑' if direction_correct else ' ↓'

            update_signal_with_resolution(cur, pred_id, accuracy_score, direction_correct, 
                                         actual_close, pred_close)

            print(f"→ Resolved! Actual close: ${actual_close:,.2f} | "
                  f"Accuracy: {accuracy_score:.1%}{direction_arrow}")

            resolved_count += 1

        conn.commit()
        print(f"\nSuccessfully resolved {resolved_count}/{len(COINS)} predictions for {target_date}")

    except Exception as e:
        print(f"\nDATABASE ERROR: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


def main():
    print("=" * 80)
    print("JOAI DAILY PREDICTIONS + SIGNALS RESOLVER")
    print(f"Run time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    resolve_predictions()

    print("=" * 80)
    print("Resolution complete")
    print("=" * 80)


if __name__ == "__main__":
    main()

# # resolve_daily_predictions.py
# # Daily job to resolve yesterday's predictions with actual candle data
# # Run once per day after midnight UTC

# import os
# import psycopg2
# from datetime import datetime, timedelta, date
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

# def get_prediction_generation_date() -> date:
#     """Date when the prediction was generated (2 days ago from now)"""
#     return (datetime.utcnow() - timedelta(days=2)).date()

# def resolve_predictions():
#     # Date when predictions were generated (e.g., Dec 19)
#     prediction_date = get_prediction_generation_date()
    
#     # The actual daily candle we want to compare against (e.g., Dec 20)
#     target_date = prediction_date + timedelta(days=1)

#     print(f"Resolving predictions generated on {prediction_date} → actual candle on {target_date}")

#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()

#         resolved_count = 0

#         for symbol in COINS:
#             coin_name = PRETTY_NAME.get(symbol, symbol)
#             print(f"  [{coin_name}] ", end="", flush=True)

#             # Step 1: Find the prediction generated on prediction_date
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

#             # Step 2: Get actual 1day candle for target_date
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

#             actual_open, actual_high, actual_low, actual_close = actual_row
#             actual_open = float(actual_open)
#             actual_high = float(actual_high)
#             actual_low = float(actual_low)
#             actual_close = float(actual_close)

#             # Step 3: Calculate accuracy based on close price
#             if actual_close == 0:
#                 accuracy = 0.0
#             else:
#                 error = abs(pred_close - actual_close) / actual_close
#                 accuracy = max(0.0, 1.0 - error)

#             accuracy_score = round(accuracy, 4)

#             # Step 4: Update the prediction row
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

#             direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
#             direction_arrow = ' ↑' if direction_correct else ' ↓'

#             print(f"→ Resolved! Actual close: ${actual_close:,.2f} | "
#                   f"Accuracy: {accuracy_score:.1%}{direction_arrow}")

#             resolved_count += 1

#         conn.commit()
#         print(f"\nSuccessfully resolved {resolved_count}/{len(COINS)} predictions for candle date {target_date}")

#     except Exception as e:
#         print(f"\nDATABASE ERROR: {e}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         cur.close()
#         conn.close()


# def main():
#     print("=" * 80)
#     print("JOAI DAILY PREDICTIONS RESOLVER")
#     print(f"Run time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("=" * 80)

#     resolve_predictions()

#     print("=" * 80)
#     print("Resolution complete")
#     print("=" * 80)


# if __name__ == "__main__":
#     main()