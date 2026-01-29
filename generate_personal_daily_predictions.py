# generate_personal_daily_predictions.py
# Daily job: Generate PERSONAL predictions + signals for a specific date
# Run via GitHub Actions or manually with date override

import os
import sys
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

JOAI_BASE_URL = os.getenv("JOAI_BASE_URL", "https://joai1.onrender.com").rstrip("/")

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

# =========================================

def get_users_with_live_adapter():
    """Get all users who have a live indicator"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT user_id
            FROM user_custom_indicators
            WHERE status = 'live'
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        print(f"Error fetching users: {e}")
        return []


def get_personal_prediction(user_id: int, symbol: str, for_date, custom_indicator_id: int) -> dict | None:
    """
    Call FastAPI /predict-by-user for one user+symbol
    Returns dict with prediction + custom_indicator_id added
    """
    try:
        print(f"  [{symbol}] User {user_id} → Requesting...", end="", flush=True)
        response = requests.post(
            f"{JOAI_BASE_URL}/predict-by-user",
            json={
                "user_id": user_id,
                "symbol": symbol,
                "timeframe": "1d"
            },
            timeout=90
        )

        if not response.ok:
            print(f" HTTP {response.status_code}")
            return None

        data = response.json()
        pred = data.get("prediction")
        if not pred or data.get("personalized") is False:
            print(" Not personalized or no data")
            return None

        print(f" Close: ${pred['close']:,.2f}")

        # Build full prediction dict + add custom_indicator_id
        full_pred = {
            "user_id": user_id,
            "symbol": symbol,
            "predicted_open": pred["open"],
            "predicted_high": pred["high"],
            "predicted_low": pred["low"],
            "predicted_close": pred["close"],
            "for_date": for_date,
            "is_personalized": True,
            "custom_indicator_id": custom_indicator_id   # ← This fixes the KeyError!
        }

        return full_pred

    except requests.exceptions.Timeout:
        print(" Timeout")
        return None
    except Exception as e:
        print(f" Error: {e}")
        return None


def derive_signal_from_prediction(pred: dict, current_close: float, atr: float) -> dict | None:
    """
    Derive forward-looking trading signal from a fresh personal prediction.
    Returns None for HOLD/no-signal cases.
    """
    if not pred or current_close <= 0 or atr <= 0:
        return None

    pred_close = pred["predicted_close"]

    # Direction with small threshold to avoid noise
    threshold = atr * 0.3  # ~0.3–0.5% move
    if pred_close > current_close + threshold:
        direction = 'LONG'
    elif pred_close < current_close - threshold:
        direction = 'SHORT'
    else:
        direction = 'HOLD'

    if direction == 'HOLD':
        return None

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
        "time_expiry": pred["for_date"] + timedelta(days=1)
    }


def save_personal_prediction_and_signal(cur, pred: dict):
    """
    Save personal prediction → get ID → derive & save signal
    All in one transaction block
    """
    # 1. Insert/upsert prediction & return its ID
    upsert_sql = """
        INSERT INTO personal_daily_predictions (
            user_id, symbol,
            predicted_open, predicted_high, predicted_low, predicted_close,
            for_date, is_personalized, custom_indicator_id
        ) VALUES (
            %(user_id)s, %(symbol)s,
            %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
            %(for_date)s, TRUE, %(custom_indicator_id)s
        )
        ON CONFLICT (user_id, symbol, for_date) DO UPDATE SET
            predicted_open = EXCLUDED.predicted_open,
            predicted_high = EXCLUDED.predicted_high,
            predicted_low = EXCLUDED.predicted_low,
            predicted_close = EXCLUDED.predicted_close,
            predicted_at = NOW(),
            custom_indicator_id = EXCLUDED.custom_indicator_id
        RETURNING id
    """

    cur.execute(upsert_sql, pred)
    prediction_id = cur.fetchone()[0]

    # 2. Get latest daily candle (close + atr) – VERY robust handling
    cur.execute("""
        SELECT close, atr
        FROM crypto_candles
        WHERE symbol = %s 
          AND timeframe = '1day'
        ORDER BY timestamp DESC
        LIMIT 1
    """, (pred["symbol"],))

    candle_row = cur.fetchone()
    if candle_row is None:
        print("  No daily candle found for this symbol → skipping signal")
        return

    current_close, atr = candle_row

    # Extra safety: ensure close is valid number
    if current_close is None:
        print("  Latest close is NULL → skipping signal")
        return

    current_close = float(current_close)

    # Handle missing or invalid ATR
    if atr is None or atr <= 0:
        print("  ATR is missing or invalid in latest candle → skipping signal")
        return

    atr = float(atr)

    # 3. Derive signal
    signal = derive_signal_from_prediction(pred, current_close, atr)
    if not signal:
        print("  HOLD → no signal generated")
        return

    # 4. Insert signal linked to personal prediction
    signal_sql = """
        INSERT INTO signals (
            custom_indicator_id,
            symbol, direction, entry_price, target_price_1, stop_loss,
            confidence_score, time_generated, time_expiry, status,
            personal_prediction_id
        ) VALUES (
            %(custom_indicator_id)s,
            %(symbol)s, %(direction)s, %(entry_price)s, %(target_price_1)s, %(stop_loss)s,
            %(confidence_score)s, NOW(), %(time_expiry)s, 'active',
            %(personal_prediction_id)s
        )
    """
    cur.execute(signal_sql, {
        **signal,
        "symbol": pred["symbol"],
        "custom_indicator_id": pred["custom_indicator_id"],  # ← Now safe!
        "personal_prediction_id": prediction_id
    })

    print(f" → Signal: {signal['direction']} (Conf: {signal['confidence_score']}%)")

    #Generate signal based on the personal prediction model; make this signal customized, for
    #example connect it only to daily predictions
    cur.execute("SELECT LAST_INSERT_ID()")
    signal_id = cur.fetchone()[0]

    signal_data = {
        "id": signal_id,
        "symbol": pred["symbol"],
        "direction": signal["direction"],
        "entry_price": signal["entry_price"],
        "target_price_1": signal["target_price_1"],
        "stop_loss": signal["stop_loss"],
        "confidence_score": signal["confidence_score"],
        "status": "active",
        "time_generated": datetime.now(timezone.utc).isoformat(),
        # add more fields
    }

    try:
        response = requests.post(
            "http://rpchost.com/broadcast-signal",
            json={"signal": signal_data},
            timeout=5
        )
        if response.status_code == 200:
            print("Signal broadcasted to Laravel")
        else:
            print(f"Broadcast failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error broadcasting signal: {e}")
    #end signal generation based on the personal model

def get_custom_indicator_id_for_user(cur, user_id: int) -> int | None:
    """Get the ID of the user's live custom indicator (the personal model ID)"""
    cur.execute("""
        SELECT id
        FROM user_custom_indicators
        WHERE user_id = %s AND status = 'live'
        LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    return row[0] if row else None


def main():
    # Optional date override via command line
    target_for_date = None
    if len(sys.argv) > 1:
        try:
            target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
            print(f"Manual override: Generating personal predictions for {target_for_date}")
        except ValueError:
            print(f"Invalid date format: {sys.argv[1]}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        target_for_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()
        print(f"Auto mode: Generating personal predictions for tomorrow → {target_for_date}")

    users = get_users_with_live_adapter()
    if not users:
        print("No users with live personal indicators.")
        return

    print(f"Generating PERSONAL predictions + signals for {target_for_date} — {len(users)} users")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        for user_id in users:
            print(f"User {user_id}")
            custom_indicator_id = get_custom_indicator_id_for_user(cur, user_id)
            if not custom_indicator_id:
                print("  No live indicator found → skipping user")
                continue

            for symbol in COINS:
                # Pass custom_indicator_id to the prediction function
                pred = get_personal_prediction(user_id, symbol, target_for_date, custom_indicator_id)
                if not pred:
                    continue

                save_personal_prediction_and_signal(cur, pred)
                print(" — done")

        conn.commit()
        print(f"\nFINISHED — processed {len(users)} users")
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