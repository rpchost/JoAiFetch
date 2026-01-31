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
    "1h": 1000,
    "4h": 1000,
    "1d": 1000
}

DESIRED_COINS = [
    "BTCUSD", "ETHUSD"
    # , "SOLUSD", "ADAUSD", "BNBUSD",
    # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
    # "LINKUSD", "AVAXUSD", "TONUSD"
]

SYMBOL_MAP = {
    "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT"
    # , "SOLUSD": "SOLUSDT",
    # "ADAUSD": "ADAUSDT", "BNBUSD": "BNBUSDT", "XRPUSD": "XRPUSDT",
    # "DOGEUSD": "DOGEUSDT", "SHIBUSD": "SHIBUSDT", "PEPEUSD": "PEPEUSDT",
    # "LINKUSD": "LINKUSDT", "AVAXUSD": "AVAXUSDT", "TONUSD": "TONUSDT"
}

PRETTY_NAME = {
    "BTCUSD": "Bitcoin",
    "ETHUSD": "Ethereum",
    # "SOLUSD": "Solana",
    # "ADAUSD": "Cardano",
    # "BNBUSD": "Binance Coin",
    # "XRPUSD": "Ripple",
    # "DOGEUSD": "Dogecoin",
    # "SHIBUSD": "Shiba Inu",
    # "PEPEUSD": "Pepe",
    # "LINKUSD": "Chainlink",
    # "AVAXUSD": "Avalanche",
    # "TONUSD": "Toncoin"
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

        # === CALCULATE ATR (14-period standard) ===
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['atr'] = true_range.rolling(window=14, min_periods=1).mean()  # min_periods=1 for early rows

        last_date = df['timestamp'].iloc[-1].date() if not df.empty else "none"
        print(f"  ✅ GOT {len(df)} candles → up to {last_date} | ATR range: {df['atr'].min():.2f} - {df['atr'].max():.2f}")

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
        INSERT INTO crypto_candles (symbol, timeframe, timestamp, open, high, low, close, volume, atr)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            atr = EXCLUDED.atr
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
                float(row['volume']),
                float(row['atr']) if 'atr' in row and pd.notna(row['atr']) else None
            ))
            if cur.rowcount > 0:
                inserted += 1

        conn.commit()
        print(f"   💾 STORED/UPDATED {inserted} rows @ {db_tf} (with ATR)")
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