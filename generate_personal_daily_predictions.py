# generate_personal_daily_predictions.py
# Daily job: Generate PERSONAL predictions + signals for tomorrow
# Supports multiple timeframes — run once per day (00:15 UTC)

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
    "BTCUSD", "ETHUSD", 
    # "SOLUSD", "ADAUSD", "BNBUSD",
    # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    # "LINKUSD", "AVAXUSD", "TONUSD"
]

TIMEFRAMES = [
    "1 day", "1 hour",
    # Add more later: "5 minutes", "15 minutes", "4 hours"
]

PRETTY_NAME = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", 
    # ...
}

# =========================================

def get_users_with_live_adapter():
    """Get all users who have a live custom indicator"""
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


def get_personal_prediction(user_id: int, symbol: str, timeframe: str, for_date, custom_indicator_id: int) -> dict | None:
    """
    Call FastAPI /predict-by-user for one user+symbol+timeframe
    Returns dict with prediction + custom_indicator_id + timeframe
    """
    try:
        query = f"predict {symbol} next {timeframe}"
        print(f"  [User {user_id}] [{symbol} {timeframe}] → {query}", end="", flush=True)

        response = requests.post(
            f"{JOAI_BASE_URL}/predict-by-user",
            json={
                "user_id": user_id,
                "symbol": symbol,
                "timeframe": timeframe
            },
            timeout=90
        )

        if not response.ok:
            print(f" HTTP {response.status_code}")
            return None

        data = response.json()
        pred = data.get("prediction")
        if not pred or data.get("personalized") is False:
            print(" Not personalized or no data111 ", pred, data)
            return None

        print(f" Close: ${pred['close']:,.2f}")

        return {
            "user_id": user_id,
            "symbol": symbol,
            "timeframe": timeframe,  # ← now saved correctly
            "predicted_open": pred["open"],
            "predicted_high": pred["high"],
            "predicted_low": pred["low"],
            "predicted_close": pred["close"],
            "for_date": for_date,
            "is_personalized": True,
            "custom_indicator_id": custom_indicator_id
        }

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

    threshold = atr * 0.3
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

    move_pct = abs(pred_close - current_close) / current_close * 100
    confidence = min(100.0, 50.0 + move_pct * 10)

    return {
        "direction": direction,
        "entry_price": entry_price,
        "target_price_1": target_price_1,
        "stop_loss": stop_loss,
        "confidence_score": round(confidence, 2),
        "time_expiry": pred["for_date"] + timedelta(days=1)
    }


def save_personal_prediction(cur, pred: dict):
    """
    Save personal prediction → get ID → derive & save signal
    Uses ON CONFLICT (user_id, symbol, for_date)
    All in one transaction block
    """
    try:
        cur.execute("BEGIN;")

        # 1. Upsert prediction (unique per user/symbol/date)
        upsert_sql = """
            INSERT INTO personal_daily_predictions (
                user_id, symbol, timeframe,
                predicted_open, predicted_high, predicted_low, predicted_close,
                for_date, is_personalized, custom_indicator_id
            ) VALUES (
                %(user_id)s, %(symbol)s, %(timeframe)s,
                %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
                %(for_date)s, TRUE, %(custom_indicator_id)s
            )
            ON CONFLICT (user_id, symbol, timeframe, for_date) DO UPDATE SET
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
        print (f"[User {pred['user_id']}] Prediction ID: {prediction_id}")

        # 2. Get latest candle for this symbol + timeframe
        cur.execute("""
            SELECT close, atr
            FROM crypto_candles
            WHERE symbol = %s 
              AND timeframe = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (pred["symbol"], pred["timeframe"]))

        candle_row = cur.fetchone()
        if not candle_row or candle_row[0] is None:
            print("  No valid candle found → skipping signal")
            cur.execute("COMMIT;")
            return

        current_close, atr = candle_row
        current_close = float(current_close)
        atr = float(atr) if atr is not None else 0

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error saving personal prediction/signal: {e}")
        import traceback
        traceback.print_exc()


def get_custom_indicator_id_for_user(cur, user_id: int) -> int | None:
    """Get the ID of the user's live custom indicator"""
    cur.execute("""
        SELECT id
        FROM user_custom_indicators
        WHERE user_id = %s AND status = 'live'
        LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    return row[0] if row else None


def main():
    # Optional date override
    target_for_date = None
    if len(sys.argv) > 1:
        try:
            target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
            print(f"Manual override: Generating for {target_for_date}")
        except ValueError:
            print(f"Invalid date: {sys.argv[1]}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        target_for_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()
        print(f"Auto mode: Generating personal predictions for {target_for_date}")

    users = get_users_with_live_adapter()
    if not users:
        print("No users with live personal indicators.")
        return

    print(f"Generating PERSONAL predictions + signals for {target_for_date} — {len(users)} users")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        task_count = 0
        for user_id in users:
            print(f"\nUser {user_id}")
            custom_indicator_id = get_custom_indicator_id_for_user(cur, user_id)
            if not custom_indicator_id:
                print("  No live indicator → skipping user")
                continue

            for symbol in COINS:
                for tf in TIMEFRAMES:
                    task_count += 1
                    print(f"  [{task_count}] {symbol:<8} {tf:<12}", end="", flush=True)

                    pred = get_personal_prediction(user_id, symbol, tf, target_for_date, custom_indicator_id)
                    if not pred:
                        print(" — skipped")
                        continue

                    print(" — begin saving ", tf, pred)
                    save_personal_prediction(cur, pred)
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

# # generate_personal_daily_predictions.py
# # Daily job: Generate PERSONAL predictions + signals for a specific date
# # Run via GitHub Actions or manually with date override

# import os
# import sys
# import requests
# import psycopg2
# from datetime import datetime, timedelta, timezone
# from dotenv import load_dotenv

# load_dotenv()

# JOAI_BASE_URL = os.getenv("JOAI_BASE_URL", "https://joai1.onrender.com").rstrip("/")

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

# # =========================================

# def get_users_with_live_adapter():
#     """Get all users who have a live indicator"""
#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()
#         cur.execute("""
#             SELECT DISTINCT user_id
#             FROM user_custom_indicators
#             WHERE status = 'live'
#         """)
#         rows = cur.fetchall()
#         cur.close()
#         conn.close()
#         return [row[0] for row in rows]
#     except Exception as e:
#         print(f"Error fetching users: {e}")
#         return []


# def get_personal_prediction(user_id: int, symbol: str, for_date, custom_indicator_id: int) -> dict | None:
#     """
#     Call FastAPI /predict-by-user for one user+symbol
#     Returns dict with prediction + custom_indicator_id added
#     """
#     try:
#         print(f"  [{symbol}] User {user_id} → Requesting...", end="", flush=True)
#         response = requests.post(
#             f"{JOAI_BASE_URL}/predict-by-user",
#             json={
#                 "user_id": user_id,
#                 "symbol": symbol,
#                 "timeframe": "1d"
#             },
#             timeout=90
#         )

#         if not response.ok:
#             print(f" HTTP {response.status_code}")
#             return None

#         data = response.json()
#         pred = data.get("prediction")
#         if not pred or data.get("personalized") is False:
#             print(" Not personalized or no data")
#             return None

#         print(f" Close: ${pred['close']:,.2f}")

#         # Build full prediction dict + add custom_indicator_id
#         full_pred = {
#             "user_id": user_id,
#             "symbol": symbol,
#             "predicted_open": pred["open"],
#             "predicted_high": pred["high"],
#             "predicted_low": pred["low"],
#             "predicted_close": pred["close"],
#             "for_date": for_date,
#             "is_personalized": True,
#             "custom_indicator_id": custom_indicator_id   # ← This fixes the KeyError!
#         }

#         return full_pred

#     except requests.exceptions.Timeout:
#         print(" Timeout")
#         return None
#     except Exception as e:
#         print(f" Error: {e}")
#         return None


# def derive_signal_from_prediction(pred: dict, current_close: float, atr: float) -> dict | None:
#     """
#     Derive forward-looking trading signal from a fresh personal prediction.
#     Returns None for HOLD/no-signal cases.
#     """
#     if not pred or current_close <= 0 or atr <= 0:
#         return None

#     pred_close = pred["predicted_close"]

#     # Direction with small threshold to avoid noise
#     threshold = atr * 0.3  # ~0.3–0.5% move
#     if pred_close > current_close + threshold:
#         direction = 'LONG'
#     elif pred_close < current_close - threshold:
#         direction = 'SHORT'
#     else:
#         direction = 'HOLD'

#     if direction == 'HOLD':
#         return None

#     entry_price = current_close
#     target_price_1 = pred_close
#     stop_loss = entry_price - (atr * 1.5) if direction == 'LONG' else entry_price + (atr * 1.5)

#     # Confidence: base 50 + magnitude bonus (capped at 100)
#     move_pct = abs(pred_close - current_close) / current_close * 100
#     confidence = min(100.0, 50.0 + move_pct * 10)  # e.g. 2% move → +20 → 70%

#     return {
#         "direction": direction,
#         "entry_price": entry_price,
#         "target_price_1": target_price_1,
#         "stop_loss": stop_loss,
#         "confidence_score": round(confidence, 2),
#         "time_expiry": pred["for_date"] + timedelta(days=1)
#     }


# def save_personal_prediction_and_signal(cur, pred: dict):
#     """
#     Save personal prediction → get its ID → derive & save signal
#     All in one transaction block.
    
#     Args:
#         cur: psycopg2 cursor
#         pred: dict with user_id, symbol, predicted_*, for_date, custom_indicator_id
#         signal_data: optional dict for signal fields (if None, no signal is created)
    
#     Returns:
#         int: prediction_id (new or updated)
#     """
#     try:
#         # Begin transaction
#         cur.execute("BEGIN;")

#         # 1. Upsert prediction and get its ID
#         upsert_sql = """
#             INSERT INTO personal_daily_predictions (
#                 user_id, symbol,
#                 predicted_open, predicted_high, predicted_low, predicted_close,
#                 for_date, is_personalized, custom_indicator_id
#             ) VALUES (
#                 %(user_id)s, %(symbol)s,
#                 %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
#                 %(for_date)s, TRUE, %(custom_indicator_id)s
#             )
#             ON CONFLICT (user_id, symbol, for_date) DO UPDATE SET
#                 predicted_open = EXCLUDED.predicted_open,
#                 predicted_high = EXCLUDED.predicted_high,
#                 predicted_low = EXCLUDED.predicted_low,
#                 predicted_close = EXCLUDED.predicted_close,
#                 predicted_at = NOW(),
#                 custom_indicator_id = EXCLUDED.custom_indicator_id
#             RETURNING id
#         """

#         cur.execute(upsert_sql, pred)
#         prediction_id = cur.fetchone()[0]  # Get the returned ID

#         # Commit transaction
#         cur.execute("COMMIT;")

#         return prediction_id

#     except Exception as e:
#         # Rollback on error
#         cur.execute("ROLLBACK;")
#         print(f"Error saving prediction/signal: {e}")
#         raise  # Let caller handle or log

# def get_custom_indicator_id_for_user(cur, user_id: int) -> int | None:
#     """Get the ID of the user's live custom indicator (the personal model ID)"""
#     cur.execute("""
#         SELECT id
#         FROM user_custom_indicators
#         WHERE user_id = %s AND status = 'live'
#         LIMIT 1
#     """, (user_id,))
#     row = cur.fetchone()
#     return row[0] if row else None


# def main():
#     # Optional date override via command line
#     target_for_date = None
#     if len(sys.argv) > 1:
#         try:
#             target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
#             print(f"Manual override: Generating personal predictions for {target_for_date}")
#         except ValueError:
#             print(f"Invalid date format: {sys.argv[1]}. Use YYYY-MM-DD")
#             sys.exit(1)
#     else:
#         target_for_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()
#         print(f"Auto mode: Generating personal predictions for tomorrow → {target_for_date}")

#     users = get_users_with_live_adapter()
#     if not users:
#         print("No users with live personal indicators.")
#         return

#     print(f"Generating PERSONAL predictions + signals for {target_for_date} — {len(users)} users")

#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()

#         for user_id in users:
#             print(f"User {user_id}")
#             custom_indicator_id = get_custom_indicator_id_for_user(cur, user_id)
#             if not custom_indicator_id:
#                 print("  No live indicator found → skipping user")
#                 continue

#             for symbol in COINS:
#                 # Pass custom_indicator_id to the prediction function
#                 pred = get_personal_prediction(user_id, symbol, target_for_date, custom_indicator_id)
#                 if not pred:
#                     continue

#                 save_personal_prediction_and_signal(cur, pred)
#                 print(" — done")

#         conn.commit()
#         print(f"\nFINISHED — processed {len(users)} users")
#     except Exception as e:
#         print(f"\nCRITICAL ERROR: {e}")
#         import traceback
#         traceback.print_exc()
#         if 'conn' in locals():
#             conn.rollback()
#     finally:
#         if 'cur' in locals():
#             cur.close()
#         if 'conn' in locals():
#             conn.close()

#     print("=" * 80)


# if __name__ == "__main__":
#     main()