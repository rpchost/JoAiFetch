# generate_personal_daily_predictions.py
# Daily/Hourly job: Generate PERSONAL predictions for tomorrow
# Supports multiple timeframes — run once per day (00:15 UTC) OR hourly for 1-hour predictions
# Usage:
#   python generate_personal_daily_predictions.py           # Daily mode (1 day + 1 hour predictions)
#   python generate_personal_daily_predictions.py hourly    # Hourly mode (1 hour predictions only)

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
    "BTCUSD", "ETHUSD"
    # , "SOLUSD", "ADAUSD", "BNBUSD",
    # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    # "LINKUSD", "AVAXUSD", "TONUSD"
]

# Timeframes configuration based on mode
TIMEFRAMES_DAILY = ["1 day", "1 hour"]  # Daily mode: both timeframes
TIMEFRAMES_HOURLY = ["1 hour"]          # Hourly mode: only 1 hour

PRETTY_NAME = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum"
    # , ...
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
        print(f"  [User {user_id}] [{symbol} {timeframe}]", end="", flush=True)

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
            print(" Not personalized")
            return None

        print(f" Close: ${pred['close']:,.2f}")

        return {
            "user_id": user_id,
            "symbol": symbol,
            "timeframe": timeframe,
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


def save_personal_prediction(cur, pred: dict):
    """
    Save personal prediction to database
    Uses ON CONFLICT (user_id, symbol, timeframe, for_date)
    """
    try:
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
        print(f" → ID: {prediction_id} ✓")

    except Exception as e:
        print(f" DB Error: {e}")
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
    # Parse command line arguments
    is_hourly_mode = False
    timeframes = TIMEFRAMES_DAILY  # Default to daily mode
    mode_name = "DAILY"
    
    # Check for hourly argument
    if len(sys.argv) > 1 and sys.argv[1].lower() == "hourly":
        is_hourly_mode = True
        timeframes = TIMEFRAMES_HOURLY  # Use hourly timeframes
        mode_name = "HOURLY"

    # Target date (always tomorrow)
    target_for_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()

    users = get_users_with_live_adapter()
    if not users:
        print("No users with live personal indicators.")
        print("=" * 80)
        return

    total_tasks_per_user = len(COINS) * len(timeframes)
    total_tasks = len(users) * total_tasks_per_user

    print("=" * 80)
    print(f"JOAI {mode_name} PERSONAL PREDICTIONS GENERATOR")
    print(f"Mode: {mode_name}")
    if is_hourly_mode:
        print(f"Generating ONLY 1-hour predictions")
    else:
        print(f"Generating both 1-day and 1-hour predictions")
    print(f"Target date: {target_for_date}")
    print(f"Users: {len(users)} | Coins: {len(COINS)} | Timeframes: {', '.join(timeframes)}")
    print(f"Total tasks: {total_tasks}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        task_count = 0
        success_count = 0
        skip_count = 0

        for user_id in users:
            print(f"\n{'─' * 80}")
            print(f"User {user_id}")
            print(f"{'─' * 80}")
            
            custom_indicator_id = get_custom_indicator_id_for_user(cur, user_id)
            if not custom_indicator_id:
                print("  No live indicator → skipping user")
                skip_count += total_tasks_per_user
                continue

            # Loop through coins and use the SELECTED timeframes
            for symbol in COINS:
                for tf in timeframes:  # This now uses the correct timeframe list
                    task_count += 1
                    print(f"  [{task_count:03d}/{total_tasks}] {symbol:<8} {tf:<12}", end="", flush=True)

                    pred = get_personal_prediction(user_id, symbol, tf, target_for_date, custom_indicator_id)
                    if not pred:
                        print("")
                        skip_count += 1
                        continue

                    save_personal_prediction(cur, pred)
                    success_count += 1

        conn.commit()
        
        print("\n" + "=" * 80)
        print(f"FINISHED — {mode_name} PERSONAL PREDICTIONS")
        print(f"  Mode: {mode_name}")
        print(f"  Timeframes processed: {', '.join(timeframes)}")
        print(f"  Users processed: {len(users)}")
        print(f"  Total tasks: {total_tasks}")
        print(f"  Successful: {success_count}")
        print(f"  Skipped: {skip_count}")
        print("=" * 80)

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
        sys.exit(1)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()

# # generate_personal_daily_predictions.py
# # Daily job: Generate PERSONAL predictions + signals for tomorrow
# # Supports multiple timeframes — run once per day (00:15 UTC)

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

# TIMEFRAMES = [
#     "1 day", "1 hour",
#     # Add more later: "5 minutes", "15 minutes", "4 hours"
# ]

# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", 
#     # ...
# }

# # =========================================

# def get_users_with_live_adapter():
#     """Get all users who have a live custom indicator"""
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


# def get_personal_prediction(user_id: int, symbol: str, timeframe: str, for_date, custom_indicator_id: int) -> dict | None:
#     """
#     Call FastAPI /predict-by-user for one user+symbol+timeframe
#     Returns dict with prediction + custom_indicator_id + timeframe
#     """
#     try:
#         query = f"predict {symbol} next {timeframe}"
#         print(f"  [User {user_id}] [{symbol} {timeframe}] → {query}", end="", flush=True)

#         response = requests.post(
#             f"{JOAI_BASE_URL}/predict-by-user",
#             json={
#                 "user_id": user_id,
#                 "symbol": symbol,
#                 "timeframe": timeframe
#             },
#             timeout=90
#         )

#         if not response.ok:
#             print(f" HTTP {response.status_code}")
#             return None

#         data = response.json()
#         pred = data.get("prediction")
#         if not pred or data.get("personalized") is False:
#             print(" Not personalized or no data111 ", pred, data)
#             return None

#         print(f" Close: ${pred['close']:,.2f}")

#         return {
#             "user_id": user_id,
#             "symbol": symbol,
#             "timeframe": timeframe,  # ← now saved correctly
#             "predicted_open": pred["open"],
#             "predicted_high": pred["high"],
#             "predicted_low": pred["low"],
#             "predicted_close": pred["close"],
#             "for_date": for_date,
#             "is_personalized": True,
#             "custom_indicator_id": custom_indicator_id
#         }

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

#     threshold = atr * 0.3
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

#     move_pct = abs(pred_close - current_close) / current_close * 100
#     confidence = min(100.0, 50.0 + move_pct * 10)

#     return {
#         "direction": direction,
#         "entry_price": entry_price,
#         "target_price_1": target_price_1,
#         "stop_loss": stop_loss,
#         "confidence_score": round(confidence, 2),
#         "time_expiry": pred["for_date"] + timedelta(days=1)
#     }


# def save_personal_prediction(cur, pred: dict):
#     """
#     Save personal prediction → get ID → derive & save signal
#     Uses ON CONFLICT (user_id, symbol, for_date)
#     All in one transaction block
#     """
#     try:
#         cur.execute("BEGIN;")

#         # 1. Upsert prediction (unique per user/symbol/date)
#         upsert_sql = """
#             INSERT INTO personal_daily_predictions (
#                 user_id, symbol, timeframe,
#                 predicted_open, predicted_high, predicted_low, predicted_close,
#                 for_date, is_personalized, custom_indicator_id
#             ) VALUES (
#                 %(user_id)s, %(symbol)s, %(timeframe)s,
#                 %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
#                 %(for_date)s, TRUE, %(custom_indicator_id)s
#             )
#             ON CONFLICT (user_id, symbol, timeframe, for_date) DO UPDATE SET
#                 predicted_open = EXCLUDED.predicted_open,
#                 predicted_high = EXCLUDED.predicted_high,
#                 predicted_low = EXCLUDED.predicted_low,
#                 predicted_close = EXCLUDED.predicted_close,
#                 predicted_at = NOW(),
#                 custom_indicator_id = EXCLUDED.custom_indicator_id
#             RETURNING id
#         """

#         cur.execute(upsert_sql, pred)
#         prediction_id = cur.fetchone()[0]
#         print (f"[User {pred['user_id']}] Prediction ID: {prediction_id}")

#         # 2. Get latest candle for this symbol + timeframe
#         cur.execute("""
#             SELECT close, atr
#             FROM crypto_candles
#             WHERE symbol = %s 
#               AND timeframe = %s
#             ORDER BY timestamp DESC
#             LIMIT 1
#         """, (pred["symbol"], pred["timeframe"]))

#         candle_row = cur.fetchone()
#         if not candle_row or candle_row[0] is None:
#             print("  No valid candle found → skipping signal")
#             cur.execute("COMMIT;")
#             return

#         current_close, atr = candle_row
#         current_close = float(current_close)
#         atr = float(atr) if atr is not None else 0

#         cur.execute("COMMIT;")

#     except Exception as e:
#         cur.execute("ROLLBACK;")
#         print(f"Error saving personal prediction/signal: {e}")
#         import traceback
#         traceback.print_exc()


# def get_custom_indicator_id_for_user(cur, user_id: int) -> int | None:
#     """Get the ID of the user's live custom indicator"""
#     cur.execute("""
#         SELECT id
#         FROM user_custom_indicators
#         WHERE user_id = %s AND status = 'live'
#         LIMIT 1
#     """, (user_id,))
#     row = cur.fetchone()
#     return row[0] if row else None


# def main():
#     # Optional date override
#     target_for_date = None
#     if len(sys.argv) > 1:
#         try:
#             target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
#             print(f"Manual override: Generating for {target_for_date}")
#         except ValueError:
#             print(f"Invalid date: {sys.argv[1]}. Use YYYY-MM-DD")
#             sys.exit(1)
#     else:
#         target_for_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()
#         print(f"Auto mode: Generating personal predictions for {target_for_date}")

#     users = get_users_with_live_adapter()
#     if not users:
#         print("No users with live personal indicators.")
#         return

#     print(f"Generating PERSONAL predictions + signals for {target_for_date} — {len(users)} users")

#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()

#         task_count = 0
#         for user_id in users:
#             print(f"\nUser {user_id}")
#             custom_indicator_id = get_custom_indicator_id_for_user(cur, user_id)
#             if not custom_indicator_id:
#                 print("  No live indicator → skipping user")
#                 continue

#             for symbol in COINS:
#                 for tf in TIMEFRAMES:
#                     task_count += 1
#                     print(f"  [{task_count}] {symbol:<8} {tf:<12}", end="", flush=True)

#                     pred = get_personal_prediction(user_id, symbol, tf, target_for_date, custom_indicator_id)
#                     if not pred:
#                         print(" — skipped")
#                         continue

#                     print(" — begin saving ", tf, pred)
#                     save_personal_prediction(cur, pred)
#                     print(" — done")

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
