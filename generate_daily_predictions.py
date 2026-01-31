# generate_daily_predictions.py
# Daily job: Generate tomorrow's predictions + signals for all supported coins
# Run once per day via GitHub Actions (e.g., at 00:15 UTC)

import os
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
JOAI_BASE_URL = os.getenv("JOAI_BASE_URL", "https://joai1.onrender.com").rstrip("/")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

COINS = [
    "BTCUSD" , "ETHUSD", 
    # "SOLUSD", "ADAUSD", "BNBUSD",
    # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    # "LINKUSD", "AVAXUSD", "TONUSD"
]

PRETTY_NAME = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", 
    # "SOLUSD": "Solana",
    # "ADAUSD": "Cardano", "BNBUSD": "Binance Coin", "XRPUSD": "Ripple",
    # "DOGEUSD": "Dogecoin", "SHIBUSD": "Shiba Inu", "PEPEUSD": "Pepe",
    # "LINKUSD": "Chainlink", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin"
}

TIMEFRAMES = [
    "1 day" ,"1 hour",
]

# =========================================

def get_prediction(symbol: str, timeframe: str) -> dict | None:
    """Fetch prediction from JoAI for a specific symbol + timeframe"""
    try:
        query = f"predict {symbol} next {timeframe}"
        print(f"  [{symbol} {timeframe}] → {query}", end="", flush=True)

        response = requests.post(
            f"{JOAI_BASE_URL}/joai",
            json={
                "query": query,
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
            "timeframe": timeframe,  # Save the requested timeframe
            "predicted_open": o,
            "predicted_high": h,
            "predicted_low": l,
            "predicted_close": c,
            "for_date": (datetime.utcnow() + timedelta(days=1)).date(),
            "custom_indicator_id": None  # Global daily job → no custom ID
        }

    except Exception as e:
        print(f" Error: {e}")
        return None


def derive_signal_from_prediction(pred: dict, current_close: float, atr: float) -> dict | None:
    """
    Derive a forward-looking trading signal from a fresh prediction.
    Returns None for HOLD/no-signal cases.
    """
    if not pred or current_close <= 0 or atr <= 0:
        return None

    pred_close = pred["predicted_close"]

    # Direction with small threshold to avoid noise
    threshold = atr * 0.3  # ~0.3–0.5% move depending on volatility
    if pred_close > current_close + threshold:
        direction = 'LONG'
    elif pred_close < current_close - threshold:
        direction = 'SHORT'
    else:
        direction = 'HOLD'

    if direction == 'HOLD':
        return None  # No signal if too close

    entry_price = current_close
    target_price_1 = pred_close
    stop_loss = entry_price - (atr * 1.5) if direction == 'LONG' else entry_price + (atr * 1.5)

    # Confidence: base 50 + magnitude bonus (capped at 100)
    move_pct = abs(pred_close - current_close) / current_close * 100
    confidence = min(100.0, 50.0 + move_pct * 10)  # e.g. 2% move → +20 → 70%

    return {
        "direction": direction,
        "entry_price": entry_price,
        "target_price_1": target_price_1,
        "stop_loss": stop_loss,
        "confidence_score": round(confidence, 2),
        "time_expiry": pred["for_date"] + timedelta(days=1)  # expires next day
    }


def save_prediction(cur, pred: dict):
    """
    Save prediction → get ID → derive & save signal
    All in one transaction block
    """
    # 1. Insert/upsert prediction & return ID
    upsert_sql = """
            INSERT INTO daily_predictions (
                symbol, timeframe,
                predicted_open, predicted_high, predicted_low, predicted_close,
                predicted_at, for_date
            ) VALUES (
                %(symbol)s, %(timeframe)s,
                %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
                NOW(), %(for_date)s
            )
            ON CONFLICT (symbol, timeframe, for_date) DO UPDATE SET
                predicted_open = EXCLUDED.predicted_open,
                predicted_high = EXCLUDED.predicted_high,
                predicted_low = EXCLUDED.predicted_low,
                predicted_close = EXCLUDED.predicted_close,
                predicted_at = NOW()
            RETURNING id
        """

    cur.execute(upsert_sql, pred)
    prediction_id = cur.fetchone()[0]
    print(" prediction_id  =  ", prediction_id)
    print(" pred  =  ", pred)

    # 2. Get latest daily candle (close + atr) – more robust
    cur.execute("""
        SELECT close, atr
        FROM crypto_candles
        WHERE symbol = %s 
          AND timeframe = '1day'
        ORDER BY timestamp DESC
        LIMIT 1
    """, (pred["symbol"],))

    candle_row = cur.fetchone()
    if not candle_row or candle_row[0] is None:
        print("  No valid daily candle found (close is missing) → skipping signal")
        return


def main():
    total_tasks = len(COINS) * len(TIMEFRAMES)
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    print("=" * 80)
    print("JOAI DAILY PREDICTIONS + SIGNALS GENERATOR (MULTI-TIMEFRAME)")
    print(f"Target date: {tomorrow}")
    print(f"Coins: {len(COINS)} | Timeframes: {len(TIMEFRAMES)} | Total tasks: {total_tasks}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        task_count = 0
        for symbol in COINS:
            for tf in TIMEFRAMES:
                task_count += 1
                print(f"[{task_count:02d}/{total_tasks}] {symbol:<8} {tf:<12}", end="", flush=True)

                pred = get_prediction(symbol, tf)
                if not pred:
                    print(" — skipped")
                    continue

                print(" — begin saving")
                print(" — begin pred ",pred)
                save_prediction(cur, pred)
                print(" — done saving")

        conn.commit()
        print(f"\nFINISHED — processed {task_count} predictions/signals")

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

    print("=" * 80)


if __name__ == "__main__":
    main()