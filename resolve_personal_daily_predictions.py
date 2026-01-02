# resolve_personal_daily_predictions.py
# Daily job: Resolve yesterday's PERSONAL predictions with actual candle data
# Run once per day after midnight UTC (e.g., 00:30 UTC)

import os
import psycopg2
from datetime import datetime, timedelta
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

def get_prediction_generation_date():
    """Date when predictions were generated (2 days ago)"""
    return (datetime.utcnow() - timedelta(days=2)).date()

def resolve_personal_predictions():
    prediction_date = get_prediction_generation_date()
    target_date = prediction_date + timedelta(days=1)

    print(f"Resolving PERSONAL predictions from {prediction_date} → actual on {target_date}")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        resolved_count = 0
        user_predictions_resolved = 0

        # Get all personal predictions to resolve
        cur.execute("""
            SELECT id, user_id, symbol, predicted_open, predicted_close, resolved_at
            FROM personal_daily_predictions
            WHERE DATE(predicted_at AT TIME ZONE 'UTC') = %s
              AND resolved_at IS NULL
        """, (prediction_date,))

        rows = cur.fetchall()

        if not rows:
            print("No personal predictions to resolve.")
            conn.close()
            return

        print(f"Found {len(rows)} personal predictions to resolve")

        for row in rows:
            pred_id, user_id, symbol, pred_open, pred_close, resolved_at = row
            pred_close = float(pred_close)

            coin_name = PRETTY_NAME.get(symbol, symbol)
            print(f"  User {user_id} [{coin_name}] ", end="", flush=True)

            # Get actual candle
            cur.execute("""
                SELECT open, high, low, close
                FROM crypto_candles
                WHERE symbol = %s AND timeframe = '1day'
                  AND DATE(timestamp AT TIME ZONE 'UTC') = %s
                ORDER BY timestamp DESC LIMIT 1
            """, (symbol, target_date))

            actual_row = cur.fetchone()
            if not actual_row:
                print("→ Actual candle missing")
                continue

            actual_open, actual_high, actual_low, actual_close = actual_row
            actual_close = float(actual_close)

            # Calculate accuracy
            if actual_close == 0:
                accuracy = 0.0
            else:
                error = abs(pred_close - actual_close) / actual_close
                accuracy = max(0.0, 1.0 - error)

            accuracy_score = round(accuracy, 4)

            # Update row
            cur.execute("""
                UPDATE personal_daily_predictions
                SET actual_open = %s, actual_high = %s, actual_low = %s, actual_close = %s,
                    resolved_at = NOW(), accuracy_score = %s
                WHERE id = %s
            """, (actual_open, actual_high, actual_low, actual_close, accuracy_score, pred_id))

            direction_correct = (pred_close > pred_open) == (actual_close > actual_open)
            arrow = ' ↑' if direction_correct else ' ↓'

            print(f"→ Resolved! Acc: {accuracy_score:.1%}{arrow} (${actual_close:,.2f})")

            resolved_count += 1
            if resolved_count == 1:
                user_predictions_resolved += 1  # count per user if needed

        conn.commit()
        print(f"\nResolved {resolved_count} personal predictions")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

def main():
    print("=" * 80)
    print("JOAI PERSONAL PREDICTIONS RESOLVER")
    print(f"Run time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    resolve_personal_predictions()

    print("=" * 80)
    print("Personal resolution complete")
    print("=" * 80)

if __name__ == "__main__":
    main()