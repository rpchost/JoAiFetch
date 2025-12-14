# check_data.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

COINS = [
    "BTCUSD", "ETHUSD", "BNBUSD", "ADAUSD", "SOLUSD", "XRPUSD",
    "DOGEUSD", "SHIBUSD", "PEPEUSD", "LINKUSD", "AVAXUSD", "TONUSD"
]

TIMEFRAMES = ["1minute", "5minutes", "15minutes", "1hour", "4hours"]

conn = psycopg2.connect(os.getenv("DATABASE_URL") + "?sslmode=require")
cur = conn.cursor()

print("=" * 70)
print("DATABASE DATA CHECK")
print("=" * 70)

for coin in COINS:
    print(f"\n{coin}:")
    for tf in TIMEFRAMES:
        cur.execute("""
            SELECT COUNT(*) FROM crypto_candles 
            WHERE symbol = %s AND timeframe = %s
        """, (coin, tf))
        count = cur.fetchone()[0]
        status = "✓" if count >= 100 else "✗"
        print(f"  {status} {tf:10s}: {count:5d} candles")

conn.close()
print("\n" + "=" * 70)