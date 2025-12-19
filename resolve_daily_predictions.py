# resolve_daily_predictions.py
# Daily job to resolve yesterday's predictions with actual candle data
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

def get_yesterday_date() -> date:
    return (datetime.utcnow() - timedelta(days=1)).date()

def get_prediction_generation_date() -> date:
    """Date when the prediction was generated (2 days ago from now)"""
    return (datetime.utcnow() - timedelta(days=2)).date()

def resolve_predictions():
    prediction_date = get_prediction_generation_date()  # e.g., Dec 18 when running on Dec 20
    print(f"Resolving predictions generated on {prediction_date} (for {prediction_date + timedelta(days=1)} candle)")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        resolved_count = 0

        for symbol in COINS:
            coin_name = PRETTY_NAME.get(symbol, symbol)
            print(f"  [{coin_name}] ", end="")

            # Step 1: Find unresolved prediction for this symbol + yesterday
            cur.execute("""
            SELECT id, predicted_close 
            FROM daily_predictions 
            WHERE symbol = %s 
            AND date(predicted_at AT TIME ZONE 'UTC') = %s 
            AND resolved_at IS NULL
            """, (symbol, prediction_date))

            row = cur.fetchone()
            if not row:
                print("No pending prediction")
                continue

            pred_id, predicted_close = row

            # Step 2: Get actual candle for yesterday from crypto_candles
            cur.execute("""
                SELECT open, high, low, close 
                FROM crypto_candles 
                WHERE symbol = %s 
                  AND timeframe = '1day' 
                  AND date(timestamp) = %s
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (symbol, yesterday))

            actual_row = cur.fetchone()
            if not actual_row:
                print("Actual candle not found yet")
                continue

            actual_open, actual_high, actual_low, actual_close = actual_row

            # Step 3: Calculate accuracy (based on close price)
            if actual_close == 0:
                accuracy = 0.0
            else:
                error = abs(predicted_close - actual_close) / actual_close
                accuracy = max(0.0, 1.0 - error)  # 1.0 = perfect

            accuracy_score = round(accuracy, 4)  # e.g., 0.9632

            # Step 4: Update the row
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

            direction_correct = (predicted_close > actual_open) == (actual_close > actual_open)
            print(f"Resolved → Actual: ${actual_close:,.2f} | "
                  f"Pred: ${predicted_close:,.2f} | "
                  f"Acc: {accuracy_score:.1%}{ ' ↑' if direction_correct else ' ↓'}")

            resolved_count += 1

        conn.commit()
        cur.close()
        conn.close()

        print(f"\nResolved {resolved_count}/{len(COINS)} predictions for {yesterday}")

    except Exception as e:
        print(f"DATABASE ERROR: {e}")


def main():
    print("=" * 80)
    print("JOAI DAILY PREDICTIONS RESOLVER")
    print(f"Run time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    resolve_predictions()

    print("=" * 80)
    print("Resolution complete")
    print("=" * 80)


if __name__ == "__main__":
    main()