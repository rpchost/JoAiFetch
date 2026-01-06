# generate_personal_daily_predictions.py
# Daily job: Generate PERSONAL predictions for a specific date
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

def get_personal_prediction(user_id: int, symbol: str, for_date) -> dict | None:
    """Call FastAPI /predict-by-user for one user+symbol"""
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
        return {
            "user_id": user_id,
            "symbol": symbol,
            "predicted_open": pred["open"],
            "predicted_high": pred["high"],
            "predicted_low": pred["low"],
            "predicted_close": pred["close"],
            "for_date": for_date,
            "is_personalized": True
        }

    except requests.exceptions.Timeout:
        print(" Timeout")
        return None
    except Exception as e:
        print(f" Error: {e}")
        return None

def save_personal_predictions(predictions: list):
    if not predictions:
        print("No personal predictions to save.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO personal_daily_predictions (
                user_id, symbol,
                predicted_open, predicted_high, predicted_low, predicted_close,
                for_date, is_personalized
            ) VALUES (
                %(user_id)s, %(symbol)s,
                %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
                %(for_date)s, TRUE
            )
            ON CONFLICT (user_id, symbol, for_date) DO UPDATE SET
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
                print(f"   Failed {pred['symbol']} user {pred['user_id']} for {pred['for_date']}: {e}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"Saved {saved}/{len(predictions)} personal predictions")

    except Exception as e:
        print(f"DB ERROR: {e}")

def main():
    # === NEW: Optional date override via command line ===
    target_for_date = None
    if len(sys.argv) > 1:
        try:
            # Expecting YYYY-MM-DD
            target_for_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
            print(f"Manual override: Generating personal predictions for date {target_for_date}")
        except ValueError:
            print(f"Invalid date format: {sys.argv[1]}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Default: tomorrow
        target_for_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()
        print(f"Auto mode: Generating personal predictions for tomorrow → {target_for_date}")

    users = get_users_with_live_adapter()
    if not users:
        print("No users with live personal indicators.")
        return

    print(f"Generating PERSONAL predictions for {target_for_date} — {len(users)} users")

    all_predictions = []

    for user_id in users:
        print(f"User {user_id}")
        for symbol in COINS:
            pred = get_personal_prediction(user_id, symbol, target_for_date)
            if pred:
                all_predictions.append(pred)

    save_personal_predictions(all_predictions)
    print(f"Done — {len(all_predictions)} personal predictions generated for {target_for_date}")

if __name__ == "__main__":
    main()

# generate_personal_daily_predictions.py
# Daily job: Generate tomorrow's PERSONAL predictions by calling FastAPI /predict-by-user for each user+coin
# Run via GitHub Actions

# import os
# import requests
# import psycopg2
# from datetime import datetime, timedelta
# from dotenv import load_dotenv

# load_dotenv()

# # FastAPI URL (same as your Laravel JOAI_BASE_URL)
# JOAI_BASE_URL = os.getenv("JOAI_BASE_URL", "https://joai1.onrender.com").rstrip("/")

# DATABASE_URL = os.getenv("DATABASE_URL")
# if DATABASE_URL and "sslmode" not in DATABASE_URL:
#     DATABASE_URL += "?sslmode=require"

# COINS = [
#     "BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "BNBUSD",
#     "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
#     "LINKUSD", "AVAXUSD", "TONUSD"
# ]

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

# def get_personal_prediction(user_id: int, symbol: str) -> dict | None:
#     """Call FastAPI /predict-by-user for one user+symbol"""
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
#         return {
#             "user_id": user_id,
#             "symbol": symbol,
#             "predicted_open": pred["open"],
#             "predicted_high": pred["high"],
#             "predicted_low": pred["low"],
#             "predicted_close": pred["close"],
#             "for_date": (datetime.utcnow() + timedelta(days=1)).date(),
#             "is_personalized": True
#         }

#     except requests.exceptions.Timeout:
#         print(" Timeout")
#         return None
#     except Exception as e:
#         print(f" Error: {e}")
#         return None

# def save_personal_predictions(predictions: list):
#     if not predictions:
#         print("No personal predictions to save.")
#         return

#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor()

#         upsert_sql = """
#             INSERT INTO personal_daily_predictions (
#                 user_id, symbol,
#                 predicted_open, predicted_high, predicted_low, predicted_close,
#                 for_date, is_personalized
#             ) VALUES (
#                 %(user_id)s, %(symbol)s,
#                 %(predicted_open)s, %(predicted_high)s, %(predicted_low)s, %(predicted_close)s,
#                 %(for_date)s, TRUE
#             )
#             ON CONFLICT (user_id, symbol, for_date) DO UPDATE SET
#                 predicted_open = EXCLUDED.predicted_open,
#                 predicted_high = EXCLUDED.predicted_high,
#                 predicted_low = EXCLUDED.predicted_low,
#                 predicted_close = EXCLUDED.predicted_close,
#                 predicted_at = NOW();
#         """

#         saved = 0
#         for pred in predictions:
#             try:
#                 cur.execute(upsert_sql, pred)
#                 saved += 1
#             except Exception as e:
#                 print(f"   Failed {pred['symbol']} user {pred['user_id']}: {e}")

#         conn.commit()
#         cur.close()
#         conn.close()
#         print(f"Saved {saved}/{len(predictions)} personal predictions")

#     except Exception as e:
#         print(f"DB ERROR: {e}")

# def main():
#     users = get_users_with_live_adapter()
#     if not users:
#         print("No users with live personal indicators.")
#         return

#     tomorrow = (datetime.utcnow() + timedelta(days=1)).date()
#     print(f"Generating PERSONAL predictions for {tomorrow} — {len(users)} users")

#     all_predictions = []

#     for user_id in users:
#         print(f"User {user_id}")
#         for symbol in COINS:
#             pred = get_personal_prediction(user_id, symbol)
#             if pred:
#                 all_predictions.append(pred)

#     save_personal_predictions(all_predictions)
#     print(f"Done — {len(all_predictions)} personal predictions generated")

# if __name__ == "__main__":
#     main()