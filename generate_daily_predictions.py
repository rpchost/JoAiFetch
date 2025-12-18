# generate_daily_predictions.py
# Daily job to generate and store tomorrow's predictions for all supported coins
# Run once per day via GitHub Actions (e.g., at 00:15 UTC)

import os
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables (for local testing)
load_dotenv()

# ================= CONFIG =================
JOAI_BASE_URL = os.getenv("JOAI_BASE_URL", "https://joai1.onrender.com").rstrip("/")

# PostgreSQL connection string (with sslmode if needed)
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

# Supported coins
COINS = [
    "BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "BNBUSD",
    "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    "LINKUSD", "AVAXUSD", "TONUSD"
]

# Pretty names for logs
PRETTY_NAME = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "SOLUSD": "Solana",
    "ADAUSD": "Cardano", "BNBUSD": "Binance Coin", "XRPUSD": "Ripple",
    "DOGEUSD": "Dogecoin", "SHIBUSD": "Shiba Inu", "PEPEUSD": "Pepe",
    "LINKUSD": "Chainlink", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin"
}

# =========================================

def get_prediction(symbol: str) -> dict | None:
    """Fetch tomorrow's prediction from JoAI for a single symbol"""
    try:
        print(f"  [{symbol}] → Requesting prediction...", end="", flush=True)

        response = requests.post(
            f"{JOAI_BASE_URL}/joai",
            json={
                "query": f"predict {symbol} next day",
                "user_id": "daily_job"
            },
            timeout=60
        )

        if not response.ok:
            print(f" HTTP {response.status_code}")
            return None

        data = response.json()

        if not data.get("success") or "lstm_prediction" not in data:
            print(" No success or missing prediction")
            return None

        pred = data["lstm_prediction"]
        parsed = data.get("parsed", {})

        # Extract and validate OHLC
        try:
            o = float(pred["open"])
            h = float(pred["high"])
            l = float(pred["low"])
            c = float(pred["close"])
        except (KeyError, ValueError, TypeError):
            print(" Invalid OHLC data")
            return None

        print(f" Close: ${c:,.2f}")

        return {
            "symbol": symbol,
            "timeframe": parsed.get("timeframe", "1 day"),
            "predicted_open": o,
            "predicted_high": h,
            "predicted_low": l,
            "predicted_close": c
        }

    except requests.exceptions.Timeout:
        print(" Timeout")
        return None
    except requests.exceptions.RequestException as e:
        print(f" Request error: {e}")
        return None
    except Exception as e:
        print(f" Unexpected error: {e}")
        return None


def save_predictions_to_db(predictions: list[dict]):
    """Insert predictions into daily_predictions table with proper UPSERT"""
    if not predictions:
        print("No valid predictions to save.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO daily_predictions (
                symbol, timeframe,
                predicted_open, predicted_high, predicted_low, predicted_close,
                predicted_at
            ) VALUES (
                %(symbol)s, %(timeframe)s,
                %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
                NOW()
            )
            ON CONFLICT (symbol, (timezone('UTC', predicted_at)::date)) DO UPDATE SET
                predicted_open = EXCLUDED.predicted_open,
                predicted_high = EXCLUDED.predicted_high,
                predicted_low = EXCLUDED.predicted_low,
                predicted_close = EXCLUDED.predicted_close,
                predicted_at = NOW();
            """

        saved = 0
        for pred in predictions:
            try:
                cur.execute(upsert_sql, pred)
                saved += 1
            except Exception as e:
                print(f"   Failed to save {pred['symbol']}: {e}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"STORED {saved}/{len(predictions)} predictions in database.")

    except Exception as e:
        print(f"DATABASE ERROR: {e}")

def main():
    total_coins = len(COINS)
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    print("=" * 80)
    print("JOAI DAILY PREDICTIONS GENERATOR")
    print(f"Target date: {tomorrow}")
    print(f"Coins: {', '.join(PRETTY_NAME.get(c, c) for c in COINS)}")
    print(f"JoAI URL: {JOAI_BASE_URL}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    valid_predictions = []

    for i, symbol in enumerate(COINS, 1):
        print(f"[{i:02d}/{total_coins}] {PRETTY_NAME.get(symbol, symbol):<12}", end="")
        pred = get_prediction(symbol)
        if pred:
            valid_predictions.append(pred)

    print("-" * 80)
    save_predictions_to_db(valid_predictions)

    print("=" * 80)
    print(f"FINISHED — {len(valid_predictions)} predictions saved for {tomorrow}")
    print("=" * 80)


if __name__ == "__main__":
    main()