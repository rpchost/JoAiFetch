# fetch_data.py — FLEXIBLE BINANCE.US HARVESTER — JAN 2026
# Supports: specific symbol, specific date, or full run

import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
import os
import time
import psycopg2
import sys
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
TIMEFRAMES = {
    "1m": 1000,
    "5m": 1000,
    "15m": 1000,
    "1h": 1000,
    "4h": 1000,
    "1d": 1000
}

DESIRED_COINS = [
    "BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "BNBUSD",
    "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    "LINKUSD", "AVAXUSD", "TONUSD"
]

SYMBOL_MAP = {
    "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT", "SOLUSD": "SOLUSDT",
    "ADAUSD": "ADAUSDT", "BNBUSD": "BNBUSDT", "XRPUSD": "XRPUSDT",
    "DOGEUSD": "DOGEUSDT", "SHIBUSD": "SHIBUSDT", "PEPEUSD": "PEPEUSDT",
    "LINKUSD": "LINKUSDT", "AVAXUSD": "AVAXUSDT", "TONUSD": "TONUSDT"
}

PRETTY_NAME = {
    "BTCUSD": "Bitcoin",
    "ETHUSD": "Ethereum",
    "SOLUSD": "Solana",
    "ADAUSD": "Cardano",
    "BNBUSD": "Binance Coin",
    "XRPUSD": "Ripple",
    "DOGEUSD": "Dogecoin",
    "SHIBUSD": "Shiba Inu",
    "PEPEUSD": "Pepe",
    "LINKUSD": "Chainlink",
    "AVAXUSD": "Avalanche",
    "TONUSD": "Toncoin"
}

def fetch_ohlcv_direct(symbol: str, timeframe: str, limit: int = None, end_date=None):
    url = "https://api.binance.us/api/v3/klines"
    params = {'symbol': symbol, 'interval': timeframe}
    
    if limit:
        params['limit'] = limit
    if end_date:
        # End time = end of specified day (23:59:59.999 UTC)
        end_dt = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)
        end_ts = int(end_dt.timestamp() * 1000)
        params['endTime'] = end_ts
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code in [400, 451]:
            print(f"  ⚠️ Symbol {symbol} not available on Binance.US")
            return pd.DataFrame()
        response.raise_for_status()
        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

        # Map back to desired XXXUSD symbol
        display_symbol = next((k for k, v in SYMBOL_MAP.items() if v == symbol), symbol)
        df['symbol'] = display_symbol

        last_date = df['timestamp'].iloc[-1].date() if not df.empty else "none"
        print(f"  ✅ GOT {len(df)} candles → up to {last_date}")
        return df

    except requests.exceptions.Timeout:
        print(f"  ⏱️ TIMEOUT for {symbol} {timeframe}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ❌ ERROR {symbol} {timeframe}: {e}")
        return pd.DataFrame()

def store_candles_postgresql(df: pd.DataFrame, tf: str):
    if df.empty:
        return 0

    tf_map = {
        '1m': '1minute', '5m': '5minutes', '15m': '15minutes',
        '1h': '1hour', '4h': '4hours', '1d': '1day'
    }
    db_tf = tf_map.get(tf, '1hour')

    conn_str = os.getenv("DATABASE_URL")
    if conn_str and "sslmode" not in conn_str:
        conn_str += "?sslmode=require"

    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()

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
        print(f"   💾 STORED {inserted} new rows @ {db_tf}")
        return inserted

    except Exception as e:
        print(f"   🛑 DB ERROR: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def main():
    # === Parse command line arguments ===
    target_symbols = None
    target_date = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--date" and i + 1 < len(args):
            try:
                target_date = datetime.strptime(args[i+1], "%Y-%m-%d").date()
                print(f"Date override: Fetching data up to {target_date}")
                i += 2
            except ValueError:
                print("Invalid date format. Use YYYY-MM-DD")
                sys.exit(1)
        else:
            if target_symbols is None:
                target_symbols = []
            target_symbols.append(arg.upper())
            i += 1

    # Determine symbols to fetch
    if target_symbols:
        valid_symbols = [s for s in target_symbols if s in SYMBOL_MAP]
        if not valid_symbols:
            print(f"No valid symbols provided. Available: {', '.join(DESIRED_COINS)}")
            sys.exit(1)
        symbols_to_fetch = [SYMBOL_MAP[s] for s in valid_symbols]
        print(f"Manual mode: Fetching only {valid_symbols}")
    else:
        symbols_to_fetch = [SYMBOL_MAP[c] for c in DESIRED_COINS]
        print("Full mode: Fetching all configured coins")

    total_tasks = len(symbols_to_fetch) * len(TIMEFRAMES)
    completed = 0

    print("=" * 80)
    print("JOAI DATA HARVESTER — BINANCE.US (USDT PAIRS → USD SYMBOLS)")
    print(f"Symbols: {len(symbols_to_fetch)} | Timeframes: {len(TIMEFRAMES)} | Tasks: {total_tasks}")
    print(f"Started: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
    print("=" * 80)

    for binance_symbol in symbols_to_fetch:
        # Safe lookup
        display_coin = next((k for k, v in SYMBOL_MAP.items() if v == binance_symbol), binance_symbol)
        coin_name = PRETTY_NAME.get(display_coin, display_coin)
        print(f"\nFetching {coin_name} → using {binance_symbol}...")

        for tf, default_limit in TIMEFRAMES.items():
            completed += 1
            limit = 1000 if target_date else default_limit
            print(f"  [{completed}/{total_tasks}] {tf:<3} → ", end="", flush=True)

            df = fetch_ohlcv_direct(binance_symbol, tf, limit=limit, end_date=target_date)
            if not df.empty:
                store_candles_postgresql(df, tf)
            else:
                print("NO DATA")
            time.sleep(0.8)

    print("=" * 80)
    print("FETCH COMPLETE")
    print(f"Finished: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
    print("JoAI database updated with fresh Binance.US data")
    print("=" * 80)

if __name__ == "__main__":
    main()

# fetch_data.py — MULTI-COIN BINANCE.US HARVESTER — JAN 2026
# Now uses XXXUSDT pairs (widely supported) and maps to XXXUSD in DB

# import pandas as pd
# import requests
# from datetime import datetime
# import os
# import time
# import psycopg2
# from dotenv import load_dotenv

# load_dotenv()

# # === CONFIG ===
# TIMEFRAMES = {
#     "1m": 2000,
#     "5m": 2000,
#     "15m": 2000,
#     "1h": 2000,
#     "4h": 2000,
#     "1d": 2000
# }

# # Define which coins you want (as displayed in your app)
# DESIRED_COINS = [
#     "BTCUSD", "ETHUSD", 
#     "SOLUSD", 
#     "ADAUSD", "BNBUSD",
#     "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
#     "LINKUSD", "AVAXUSD", "TONUSD"
# ]

# # Map desired coin → actual Binance.US symbol (USDT pair)
# SYMBOL_MAP = {
#     "BTCUSD": "BTCUSDT",
#     "ETHUSD": "ETHUSDT",
#     "SOLUSD": "SOLUSDT",
#     "ADAUSD": "ADAUSDT",
#     "BNBUSD": "BNBUSDT",
#     "XRPUSD": "XRPUSDT",   # May be restricted in US — will skip if 400
#     "DOGEUSD": "DOGEUSDT",
#     "SHIBUSD": "SHIBUSDT",
#     "PEPEUSD": "PEPEUSDT", # Check if available
#     "LINKUSD": "LINKUSDT",
#     "AVAXUSD": "AVAXUSDT",
#     "TONUSD": "TONUSDT"    # Usually not on Binance.US — will skip
# }

# # Reverse map for logging
# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "SOLUSD": "Solana",
#     "ADAUSD": "Cardano", "BNBUSD": "Binance Coin", "XRPUSD": "Ripple",
#     "DOGEUSD": "Dogecoin", "SHIBUSD": "Shiba Inu", "PEPEUSD": "Pepe",
#     "LINKUSD": "Chainlink", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin"
# }

# # Actual symbols to fetch from Binance.US
# SYMBOLS_TO_FETCH = [SYMBOL_MAP[coin] for coin in DESIRED_COINS]

# def fetch_ohlcv_direct(symbol: str, timeframe: str, limit: int):
#     url = "https://api.binance.us/api/v3/klines"
#     params = {'symbol': symbol, 'interval': timeframe, 'limit': limit}
#     try:
#         response = requests.get(url, params=params, timeout=15)
#         if response.status_code == 400:
#             print(f"  ⚠️  Symbol {symbol} not supported on Binance.US (likely not listed)")
#             return pd.DataFrame()
#         if response.status_code == 451:  # Blocked in US
#             print(f"  🚫 Symbol {symbol} blocked in US region")
#             return pd.DataFrame()
#         response.raise_for_status()
#         data = response.json()
#         if not data:
#             return pd.DataFrame()

#         df = pd.DataFrame(data, columns=[
#             'timestamp', 'open', 'high', 'low', 'close', 'volume',
#             'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'
#         ])[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

#         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
#         df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        
#         # Map back to desired XXXUSD symbol for storage
#         display_symbol = [k for k, v in SYMBOL_MAP.items() if v == symbol][0]
#         df['symbol'] = display_symbol
        
#         print(f"  ✅ GOT {len(df)} candles | {df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M')} → {df['timestamp'].iloc[-1].strftime('%H:%M')}")
#         return df

#     except requests.exceptions.Timeout:
#         print(f"  ⏱️  TIMEOUT for {symbol}")
#         return pd.DataFrame()
#     except Exception as e:
#         print(f"  ❌ ERROR {symbol} {timeframe}: {e}")
#         return pd.DataFrame()

# def store_candles_postgresql(df, tf: str):
#     if df.empty:
#         return 0

#     tf_map = {
#         '1m': '1minute', '5m': '5minutes', '15m': '15minutes',
#         '1h': '1hour', '4h': '4hours', '1d': '1day'
#     }
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

#     inserted = 0
#     for _, row in df.iterrows():
#         try:
#             cur.execute(sql, (
#                 row['symbol'],
#                 db_tf,
#                 row['timestamp'],
#                 float(row['open']),
#                 float(row['high']),
#                 float(row['low']),
#                 float(row['close']),
#                 float(row['volume'])
#             ))
#             if cur.rowcount > 0:
#                 inserted += 1
#         except Exception as e:
#             print(f"     DB insert error: {e}")

#     conn.commit()
#     conn.close()
#     print(f"   💾 STORED {inserted} new rows @ {db_tf}")
#     return inserted

# def main():
#     total_tasks = len(SYMBOLS_TO_FETCH) * len(TIMEFRAMES)
#     completed = 0

#     print("=" * 80)
#     print("JOAI MULTI-COIN DATA HARVESTER — BINANCE.US (USING USDT PAIRS)")
#     print(f"Target coins: {', '.join(DESIRED_COINS)}")
#     print(f"Fetching as: {', '.join(SYMBOLS_TO_FETCH)}")
#     print(f"Timeframes: {', '.join(TIMEFRAMES.keys())}")
#     print(f"Total tasks: {total_tasks}")
#     print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
#     print("=" * 80)

#     for binance_symbol in SYMBOLS_TO_FETCH:
#         # Get display name
#         display_coin = [k for k, v in SYMBOL_MAP.items() if v == binance_symbol][0]
#         coin_name = PRETTY_NAME.get(display_coin, display_coin)
#         print(f"\nFetching {coin_name} → using {binance_symbol}...")

#         for tf, limit in TIMEFRAMES.items():
#             completed += 1
#             print(f"  [{completed}/{total_tasks}] {binance_symbol} @ {tf:<3} → ", end="", flush=True)
#             df = fetch_ohlcv_direct(binance_symbol, tf, limit)
#             if not df.empty:
#                 new_rows = store_candles_postgresql(df, tf)
#             else:
#                 print("NO DATA")
#             time.sleep(0.8)  # Polite rate limiting

#     print("=" * 80)
#     print("ALL AVAILABLE COINS UPDATED SUCCESSFULLY")
#     print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
#     print("JoAI now has fresh, reliable data from Binance.US")
#     print("=" * 80)

# if __name__ == "__main__":
#     main()

# # fetch_data.py — MULTI-COIN BINANCE.US HARVESTER — NOV 2025
# import pandas as pd
# import requests
# from datetime import datetime
# import os
# import time
# import psycopg2
# from dotenv import load_dotenv

# load_dotenv()

# # === CONFIG ===
# TIMEFRAMES = {
#     "1m": 2000,
#     "5m": 2000,
#     "15m": 2000,
#     "1h": 2000,
#     "4h": 2000,
#     "1d": 2000
# }

# # Read symbols from .env (comma-separated)
# SYMBOLS_RAW = os.getenv("CRYPTO_SYMBOLS", "BTCUSD")  # fallback to BTCUSD only
# SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()]

# # Mapping for pretty names in logs (optional)
# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "SOLUSD": "Solana",
#     "ADAUSD": "Cardano", "DOGEUSD": "Dogecoin", "XRPUSD": "Ripple",
#     "BNBUSD": "Binance Coin", "AVAXUSD": "Avalanche", "TONUSD": "Toncoin",
#     "LINKUSD": "Chainlink"
# }

# def fetch_ohlcv_direct(symbol: str, timeframe: str, limit: int):
#     url = "https://api.binance.us/api/v3/klines"
#     params = {'symbol': symbol, 'interval': timeframe, 'limit': limit}
#     try:
#         response = requests.get(url, params=params, timeout=10)
#         if response.status_code == 400:
#             print(f"  Symbol {symbol} not supported on Binance.US")
#             return pd.DataFrame()
#         response.raise_for_status()
#         data = response.json()
#         if not data:
#             return pd.DataFrame()

#         df = pd.DataFrame(data, columns=[
#             'timestamp', 'open', 'high', 'low', 'close', 'volume',
#             'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'
#         ])[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

#         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
#         df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
#         df['symbol'] = symbol
#         print(f"  GOT {len(df)} candles | {df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M')} → {df['timestamp'].iloc[-1].strftime('%H:%M')}")
#         return df
#     except requests.exceptions.Timeout:
#         print(f"  TIMEOUT for {symbol} {timeframe} — skipping")
#         return pd.DataFrame()
#     except Exception as e:
#         print(f"  ERROR {symbol} {timeframe}: {e}")
#         return pd.DataFrame()

# def store_candles_postgresql(df, tf: str):
#     if df.empty:
#         return 0

#     tf_map = {
#         '1m': '1minute', '5m': '5minutes', '15m': '15minutes',
#         '1h': '1hour', '4h': '4hours',
#         '1d': '1day'
#     }
#     db_tf = tf_map.get(tf, '1hour')

#     conn_str = os.getenv("DATABASE_URL")
#     if conn_str and "sslmode" not in conn_str:
#         conn_str += "?sslmode=require"

#     conn = psycopg2.connect(conn_str)
#     cur = conn.cursor()

#     # THIS WORKS 100% OF THE TIME — NO MATTER THE CONSTRAINT NAME
#     sql = """
#     INSERT INTO crypto_candles (symbol, timeframe, timestamp, open, high, low, close, volume)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING
#     """

#     inserted = 0
#     for _, row in df.iterrows():
#         cur.execute(sql, (
#             row['symbol'],
#             db_tf,
#             row['timestamp'],
#             float(row['open']),
#             float(row['high']),
#             float(row['low']),
#             float(row['close']),
#             float(row['volume'])
#         ))
#         if cur.rowcount > 0:
#             inserted += 1

#     conn.commit()
#     conn.close()
#     print(f"   STORED {inserted} new rows @ {db_tf}")
#     return inserted

# def main():
#     total_tasks = len(SYMBOLS) * len(TIMEFRAMES)
#     completed = 0

#     print("=" * 80)
#     print("JOAI MULTI-COIN DATA HARVESTER — BINANCE.US — LIVE")
#     print(f"Coins: {', '.join(f'{PRETTY_NAME.get(s,s)} ({s})' for s in SYMBOLS)}")
#     print(f"Timeframes: {', '.join(TIMEFRAMES.keys())}")
#     print(f"Total tasks: {total_tasks}")
#     print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
#     print("=" * 80)

#     for symbol in SYMBOLS:
#         coin_name = PRETTY_NAME.get(symbol, symbol)
#         print(f"\nFetching {coin_name} ({symbol})...")
#         for tf, limit in TIMEFRAMES.items():
#             completed += 1
#             print(f"  [{completed}/{total_tasks}] {symbol} @ {tf:<3} → ", end="", flush=True)
#             df = fetch_ohlcv_direct(symbol, tf, limit)
#             if not df.empty:
#                 new_rows = store_candles_postgresql(df, tf)
#                 print(f"STORED {new_rows} new rows")
#             else:
#                 print("NO DATA")
#             time.sleep(0.8)  # Stay under rate limits

#     print("=" * 80)
#     print("ALL COINS UPDATED SUCCESSFULLY")
#     print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
#     print("JoAI is now fully armed with fresh Binance.US data")
#     print("=" * 80)

# if __name__ == "__main__":
#     main()