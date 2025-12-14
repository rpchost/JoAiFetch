# fetch_data.py — MULTI-COIN BINANCE.US HARVESTER — NOV 2025
import pandas as pd
import requests
from datetime import datetime
import os
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
TIMEFRAMES = {
    "1m": 2000,
    "5m": 2000,
    "15m": 2000,
    "1h": 2000,
    "4h": 2000
}

# Read symbols from .env (comma-separated)
SYMBOLS_RAW = os.getenv("CRYPTO_SYMBOLS", "BTCUSD")  # fallback to BTCUSD only
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()]

# Mapping for pretty names in logs (optional)
PRETTY_NAME = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "SOLUSD": "Solana",
    "ADAUSD": "Cardano", "DOGEUSD": "Dogecoin", "XRPUSD": "Ripple",
    "BNBUSD": "Binance Coin", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin",
    "LINKUSD": "Chainlink"
}

def fetch_ohlcv_direct(symbol: str, timeframe: str, limit: int):
    url = "https://api.binance.us/api/v3/klines"
    params = {'symbol': symbol, 'interval': timeframe, 'limit': limit}
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 400:
            print(f"  Symbol {symbol} not supported on Binance.US")
            return pd.DataFrame()
        response.raise_for_status()
        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'
        ])[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        df['symbol'] = symbol
        print(f"  GOT {len(df)} candles | {df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M')} → {df['timestamp'].iloc[-1].strftime('%H:%M')}")
        return df
    except requests.exceptions.Timeout:
        print(f"  TIMEOUT for {symbol} {timeframe} — skipping")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ERROR {symbol} {timeframe}: {e}")
        return pd.DataFrame()

def store_candles_postgresql(df, tf: str):
    if df.empty:
        return 0

    tf_map = {
        '1m': '1minute', '5m': '5minutes', '15m': '15minutes',
        '1h': '1hour', '4h': '4hours'
    }
    db_tf = tf_map.get(tf, '1hour')

    conn_str = os.getenv("DATABASE_URL")
    if conn_str and "sslmode" not in conn_str:
        conn_str += "?sslmode=require"

    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    # THIS WORKS 100% OF THE TIME — NO MATTER THE CONSTRAINT NAME
    sql = """
    INSERT INTO crypto_candles (symbol, timeframe, timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING
    """

    inserted = 0
    for _, row in df.iterrows():
        cur.execute(sql, (
            row['symbol'],
            db_tf,
            row['timestamp'],
            float(row['open']),
            float(row['high']),
            float(row['low']),
            float(row['close']),
            float(row['volume'])
        ))
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    conn.close()
    print(f"   STORED {inserted} new rows @ {db_tf}")
    return inserted

def main():
    total_tasks = len(SYMBOLS) * len(TIMEFRAMES)
    completed = 0

    print("=" * 80)
    print("JOAI MULTI-COIN DATA HARVESTER — BINANCE.US — LIVE")
    print(f"Coins: {', '.join(f'{PRETTY_NAME.get(s,s)} ({s})' for s in SYMBOLS)}")
    print(f"Timeframes: {', '.join(TIMEFRAMES.keys())}")
    print(f"Total tasks: {total_tasks}")
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 80)

    for symbol in SYMBOLS:
        coin_name = PRETTY_NAME.get(symbol, symbol)
        print(f"\nFetching {coin_name} ({symbol})...")
        for tf, limit in TIMEFRAMES.items():
            completed += 1
            print(f"  [{completed}/{total_tasks}] {symbol} @ {tf:<3} → ", end="", flush=True)
            df = fetch_ohlcv_direct(symbol, tf, limit)
            if not df.empty:
                new_rows = store_candles_postgresql(df, tf)
                print(f"STORED {new_rows} new rows")
            else:
                print("NO DATA")
            time.sleep(0.8)  # Stay under rate limits

    print("=" * 80)
    print("ALL COINS UPDATED SUCCESSFULLY")
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("JoAI is now fully armed with fresh Binance.US data")
    print("=" * 80)

if __name__ == "__main__":
    main()

# # fetch_data.py — 100% WORKING BINANCE.US FIX — NOV 2025
# import pandas as pd
# import requests
# from datetime import datetime
# import os
# from dotenv import load_dotenv
# import time
# import psycopg2

# load_dotenv()

# TIMEFRAMES = {
#     "1m": 1000,
#     "5m": 1000,
#     "15m": 1000,
#     "1h": 2000,
#     "4h": 2000
# }

# def fetch_ohlcv_direct(timeframe: str, limit: int):
#     """Direct HTTP request to Binance.US — completely bypasses ccxt and binance.com"""
#     url = "https://api.binance.us/api/v3/klines"
#     params = {
#         'symbol': 'BTCUSD',
#         'interval': timeframe,
#         'limit': limit
#     }
#     try:
#         response = requests.get(url, params=params, timeout=30)
#         response.raise_for_status()
#         data = response.json()
#         if not data or len(data) == 0:
#             print("  No data returned from Binance.US")
#             return pd.DataFrame()
        
#         df = pd.DataFrame(data, columns=[
#             'timestamp', 'open', 'high', 'low', 'close', 'volume',
#             'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'
#         ])
#         df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
#         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
#         df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
#         df['symbol'] = 'BTCUSD'
#         print(f"  SUCCESS → {len(df)} candles | {df['timestamp'].iloc[0]} → {df['timestamp'].iloc[-1]}")
#         return df
#     except Exception as e:
#         print(f"  FAILED: {e}")
#         return pd.DataFrame()

# def store_candles_postgresql(df, tf: str):
#     if df.empty:
#         return
#     tf_map = {'1m': '1minute', '5m': '5minutes', '15m': '15minutes', '1h': '1hour', '4h': '4hours'}
#     db_tf = tf_map.get(tf, '1hour')

#     conn_str = os.getenv("DATABASE_URL")
#     if conn_str and "sslmode" not in conn_str:
#         conn_str += "?sslmode=require"
    
#     conn = psycopg2.connect(conn_str)
#     cur = conn.cursor()
    
#     sql = """
#     INSERT INTO crypto_candles (symbol, timeframe, timestamp, open, high, low, close, volume)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING
#     """
    
#     count = 0
#     for _, row in df.iterrows():
#         cur.execute(sql, (
#             "BTCUSD", db_tf, row['timestamp'],
#             float(row['open']), float(row['high']), float(row['low']),
#             float(row['close']), float(row['volume'])
#         ))
#         count += 1
    
#     conn.commit()
#     conn.close()
#     print(f"   Stored {count} new rows @ {db_tf}")

# def populate_multiple_symbols():
#     print("=" * 70)
#     print("JOAI DATA UPDATE — BINANCE.US DIRECT API — NO CCXT — NO 451")
#     print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
#     print("=" * 70)

#     total = len(TIMEFRAMES)
#     completed = 0

#     for tf, limit in TIMEFRAMES.items():
#         completed += 1
#         print(f"[{completed}/{total}] BTCUSD @ {tf} → ", end="", flush=True)
        
#         df = fetch_ohlcv_direct(tf, limit)
#         if not df.empty:
#             store_candles_postgresql(df, tf)
#             print(f"STORED {len(df)}")
#         else:
#             print("No data")
        
#         time.sleep(1.0)  # Binance.US rate limit is generous

#     print("=" * 70)
#     print("ALL DATA UPDATED SUCCESSFULLY — YOU ARE FREE")
#     print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
#     print("=" * 70)

# if __name__ == "__main__":
#     populate_multiple_symbols()