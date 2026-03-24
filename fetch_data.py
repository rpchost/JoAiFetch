# fetch_data.py
# Robust Binance.US harvester:
# - Timeframes: 1h, 4h, 1d only
# - Backfill to 10,000 candles once per symbol/timeframe (if missing)
# - Then incremental fetch only (plus overlap healing window)
# - Batch upsert to PostgreSQL
#
# Optional args:
#   python fetch_data.py                 # all configured symbols
#   python fetch_data.py BTCUSD ETHUSD  # specific symbols
#
# Env:
#   DATABASE_URL=...

import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG ----------------
DESIRED_COINS = ["BTCUSD", "ETHUSD"]
SYMBOL_MAP = {"BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT"}

# Binance interval -> DB timeframe
TIMEFRAMES = {
    "1h": "1hour",
    "4h": "4hours",
    "1d": "1day",
}

INTERVAL_MS = {
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

HISTORY_TARGET = 10_000
BINANCE_MAX_LIMIT = 1000

# overlap healing to catch missed/late candles and recalc ATR consistently
OVERLAP_BARS = {
    "1h": 240,   # 10 days
    "4h": 120,   # 20 days
    "1d": 60,    # 2 months
}

REQUEST_TIMEOUT_SEC = 20
REQUEST_MAX_RETRIES = 5
REQUEST_BACKOFF_SEC = 1.2


# ---------------- HELPERS ----------------
def get_db_conn_str() -> str:
    conn_str = os.getenv("DATABASE_URL", "").strip()
    if not conn_str:
        raise RuntimeError("DATABASE_URL is not set")
    if "sslmode" not in conn_str:
        conn_str += "?sslmode=require"
    return conn_str


def binance_to_display_symbol(binance_symbol: str) -> str:
    for k, v in SYMBOL_MAP.items():
        if v == binance_symbol:
            return k
    return binance_symbol


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def latest_closed_end_ms(tf: str) -> int:
    step = INTERVAL_MS[tf]
    current_open = (now_ms() // step) * step
    return current_open - 1  # exclude current open candle


def parse_binance_klines(data: list, binance_symbol: str) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(
        data,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ],
    )
    df = df[["open_time", "open", "high", "low", "close", "volume"]].copy()
    df.rename(columns={"open_time": "timestamp"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["symbol"] = binance_to_display_symbol(binance_symbol)
    df.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"], inplace=True)
    return df


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.sort_values("timestamp").copy()
    high_low = out["high"] - out["low"]
    high_close = (out["high"] - out["close"].shift(1)).abs()
    low_close = (out["low"] - out["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    out["atr"] = tr.rolling(window=period, min_periods=1).mean()
    return out


def request_klines(
    symbol: str,
    interval: str,
    limit: int = BINANCE_MAX_LIMIT,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
) -> pd.DataFrame:
    url = "https://api.binance.us/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": int(limit)}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)

    last_err = None
    for attempt in range(1, REQUEST_MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            if r.status_code in (400, 451):
                print(f"  ⚠️ {symbol} {interval} unavailable on Binance.US (status {r.status_code})")
                return pd.DataFrame()
            r.raise_for_status()
            data = r.json()
            return parse_binance_klines(data, symbol)
        except Exception as e:
            last_err = e
            sleep_s = REQUEST_BACKOFF_SEC * attempt
            print(f"  ⏱️ API retry {attempt}/{REQUEST_MAX_RETRIES} for {symbol} {interval}: {e}")
            time.sleep(sleep_s)

    print(f"  ❌ API failed for {symbol} {interval}: {last_err}")
    return pd.DataFrame()


def get_db_state(conn, symbol_display: str, db_tf: str) -> Tuple[int, Optional[datetime]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint, MAX(timestamp)
            FROM crypto_candles
            WHERE symbol = %s AND timeframe = %s
            """,
            (symbol_display, db_tf),
        )
        cnt, mx = cur.fetchone()
        return int(cnt or 0), mx


def upsert_candles(conn, df: pd.DataFrame, tf: str) -> int:
    if df.empty:
        return 0

    db_tf = TIMEFRAMES[tf]
    values = [
        (
            row.symbol,
            db_tf,
            row.timestamp.to_pydatetime(),
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            float(row.volume),
            float(row.atr) if pd.notna(row.atr) else None,
        )
        for row in df.itertuples(index=False)
    ]

    sql = """
    INSERT INTO crypto_candles
      (symbol, timeframe, timestamp, open, high, low, close, volume, atr)
    VALUES %s
    ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      atr = EXCLUDED.atr
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            sql,
            values,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            page_size=1000,
        )
    conn.commit()
    return len(values)


def fetch_backfill_missing(symbol_binance: str, tf: str, missing_count: int) -> pd.DataFrame:
    # Pull older chunks backward by endTime
    to_fetch = max(0, missing_count)
    if to_fetch == 0:
        return pd.DataFrame()

    end_ms = latest_closed_end_ms(tf)
    chunks: List[pd.DataFrame] = []
    fetched = 0

    while fetched < to_fetch:
        take = min(BINANCE_MAX_LIMIT, to_fetch - fetched)
        chunk = request_klines(symbol_binance, tf, limit=take, end_ms=end_ms)
        if chunk.empty:
            break

        chunks.append(chunk)
        fetched += len(chunk)

        first_open_ms = int(chunk["timestamp"].iloc[0].timestamp() * 1000)
        end_ms = first_open_ms - 1

        if len(chunk) < take:
            break

    if not chunks:
        return pd.DataFrame()

    out = pd.concat(chunks[::-1], ignore_index=True)
    out = out.drop_duplicates(subset=["symbol", "timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return out


def fetch_incremental_with_overlap(
    symbol_binance: str,
    tf: str,
    max_ts: Optional[datetime],
) -> pd.DataFrame:
    if max_ts is None:
        return pd.DataFrame()

    step = INTERVAL_MS[tf]
    overlap = OVERLAP_BARS[tf]
    max_ms = int(max_ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
    start_ms = max(0, max_ms - overlap * step)
    end_ms = latest_closed_end_ms(tf)
    if start_ms > end_ms:
        return pd.DataFrame()

    chunks: List[pd.DataFrame] = []
    cursor = start_ms

    while cursor <= end_ms:
        chunk = request_klines(symbol_binance, tf, limit=BINANCE_MAX_LIMIT, start_ms=cursor, end_ms=end_ms)
        if chunk.empty:
            break
        chunks.append(chunk)

        last_open_ms = int(chunk["timestamp"].iloc[-1].timestamp() * 1000)
        next_cursor = last_open_ms + step
        if next_cursor <= cursor:
            break
        cursor = next_cursor

        if len(chunk) < BINANCE_MAX_LIMIT:
            break

    if not chunks:
        return pd.DataFrame()

    out = pd.concat(chunks, ignore_index=True)
    out = out.drop_duplicates(subset=["symbol", "timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return out


def ensure_unique_index(conn):
    # Important for ON CONFLICT correctness.
    # Run once safely; this is idempotent.
    with conn.cursor() as cur:
        cur.execute(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname = 'ux_crypto_candles_symbol_tf_ts'
              ) THEN
                CREATE UNIQUE INDEX ux_crypto_candles_symbol_tf_ts
                ON crypto_candles(symbol, timeframe, timestamp);
              END IF;
            END $$;
            """
        )
    conn.commit()


def main():
    args = [a.upper() for a in sys.argv[1:]]
    if args:
        desired = [s for s in args if s in SYMBOL_MAP]
        if not desired:
            print(f"No valid symbols in args. Allowed: {', '.join(DESIRED_COINS)}")
            sys.exit(1)
        symbols_binance = [SYMBOL_MAP[s] for s in desired]
    else:
        symbols_binance = [SYMBOL_MAP[s] for s in DESIRED_COINS]

    conn_str = get_db_conn_str()
    conn = psycopg2.connect(conn_str)

    try:
        ensure_unique_index(conn)

        total_tasks = len(symbols_binance) * len(TIMEFRAMES)
        task_no = 0
        total_rows = 0
        t0 = time.time()

        print("=" * 80)
        print("JOAI CANDLE HARVESTER (1h/4h/1d) — 10k bootstrap + incremental")
        print(f"Symbols: {len(symbols_binance)} | Timeframes: {len(TIMEFRAMES)} | Tasks: {total_tasks}")
        print(f"Started: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
        print("=" * 80)

        for sym_b in symbols_binance:
            sym_disp = binance_to_display_symbol(sym_b)
            print(f"\n[{sym_disp}]")

            for tf, db_tf in TIMEFRAMES.items():
                task_no += 1
                print(f"  [{task_no}/{total_tasks}] {tf} ...", end=" ", flush=True)

                count, max_ts = get_db_state(conn, sym_disp, db_tf)

                if count < HISTORY_TARGET:
                    missing = HISTORY_TARGET - count
                    df = fetch_backfill_missing(sym_b, tf, missing)
                    mode = f"backfill_missing={missing}"
                else:
                    df = fetch_incremental_with_overlap(sym_b, tf, max_ts)
                    mode = "incremental+overlap"

                if df.empty:
                    print(f"no new rows ({mode})")
                    continue

                df = add_atr(df)
                n = upsert_candles(conn, df, tf)
                total_rows += n

                last_ts = df["timestamp"].max()
                print(f"stored={n} ({mode}) up_to={last_ts}")

                # small pacing
                time.sleep(0.12)

        dt = time.time() - t0
        print("\n" + "=" * 80)
        print("DONE")
        print(f"Rows upserted: {total_rows}")
        print(f"Elapsed: {dt:.2f}s")
        print(f"Finished: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
        print("=" * 80)

    finally:
        conn.close()


if __name__ == "__main__":
    main()


# # fetch_data.py — OPTIMIZED BINANCE.US HARVESTER WITH SIGNAL MONITORING
# # Optimizations:
# # 1. Batch database inserts (100x faster)
# # 2. Reduced sleep delays
# # 3. Concurrent API calls (optional)

# import pandas as pd
# import requests
# from datetime import datetime, timedelta, timezone
# import os
# import time
# import psycopg2
# import psycopg2.extras
# import sys
# from dotenv import load_dotenv
# import asyncio
# import asyncpg

# load_dotenv()

# # === CONFIG ===
# TIMEFRAMES = {
#     "1h": 1000,
#     "4h": 1000,
#     "1d": 1000
# }

# DESIRED_COINS = [
#     "BTCUSD", "ETHUSD"
# ]

# SYMBOL_MAP = {
#     "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT"
# }

# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin",
#     "ETHUSD": "Ethereum",
# }


# # ==================== SIGNAL MONITORING LOGIC ====================

# async def monitor_and_update_signals(latest_candles: dict):
#     """
#     Monitor active signals and update them if target or stop loss is hit.
#     """
#     conn_str = os.getenv("DATABASE_URL")
#     if not conn_str:
#         print("  ⚠️ DATABASE_URL not set, skipping signal monitoring")
#         return {'total': 0, 'updated': 0}
    
#     if "sslmode" not in conn_str:
#         conn_str += "?sslmode=require"
    
#     stats = {
#         'total_active': 0,
#         'hit_target': 0,
#         'hit_sl': 0,
#         'still_active': 0
#     }
    
#     try:
#         conn = await asyncpg.connect(conn_str)
        
#         # Fetch all active signals
#         signals = await conn.fetch("""
#             SELECT 
#                 id, symbol, direction, entry_price,
#                 target_price_1, target_price_2, target_price_3,
#                 stop_loss, confidence_score, time_generated
#             FROM signals
#             WHERE status = 'active'
#             ORDER BY time_generated DESC
#         """)
        
#         stats['total_active'] = len(signals)
        
#         if not signals:
#             await conn.close()
#             return stats
        
#         print(f"\n  📊 Monitoring {len(signals)} active signals...")
        
#         for signal in signals:
#             symbol = signal['symbol']
            
#             if symbol not in latest_candles:
#                 continue
            
#             candle = latest_candles[symbol]
#             high = candle['high']
#             low = candle['low']
#             close = candle['close']
            
#             direction = signal['direction']
#             target_1 = signal['target_price_1']
#             target_2 = signal['target_price_2']
#             target_3 = signal['target_price_3']
#             sl = signal['stop_loss']
            
#             new_status = None
#             hit_price = None
            
#             if direction == 'LONG':
#                 if sl is not None and low <= sl:
#                     new_status = 'hit_sl'
#                     hit_price = sl
#                 elif target_3 is not None and high >= target_3:
#                     new_status = 'hit_target'
#                     hit_price = target_3
#                 elif target_2 is not None and high >= target_2:
#                     new_status = 'hit_target'
#                     hit_price = target_2
#                 elif target_1 is not None and high >= target_1:
#                     new_status = 'hit_target'
#                     hit_price = target_1
            
#             elif direction == 'SHORT':
#                 if sl is not None and high >= sl:
#                     new_status = 'hit_sl'
#                     hit_price = sl
#                 elif target_3 is not None and low <= target_3:
#                     new_status = 'hit_target'
#                     hit_price = target_3
#                 elif target_2 is not None and low <= target_2:
#                     new_status = 'hit_target'
#                     hit_price = target_2
#                 elif target_1 is not None and low <= target_1:
#                     new_status = 'hit_target'
#                     hit_price = target_1
            
#             if new_status:
#                 await conn.execute("""
#                     UPDATE signals
#                     SET 
#                         status = $1,
#                         updated_at = NOW()
#                     WHERE id = $2
#                 """, new_status, signal['id'])
                
#                 if new_status == 'hit_target':
#                     stats['hit_target'] += 1
#                     print(f"    ✅ Signal #{signal['id']} ({symbol} {direction}) HIT TARGET @ ${hit_price:,.2f}")
#                 elif new_status == 'hit_sl':
#                     stats['hit_sl'] += 1
#                     print(f"    ❌ Signal #{signal['id']} ({symbol} {direction}) HIT STOP LOSS @ ${hit_price:,.2f}")
#             else:
#                 stats['still_active'] += 1
        
#         await conn.close()
        
#         if stats['hit_target'] > 0 or stats['hit_sl'] > 0:
#             print(f"  📈 Signal Update Summary:")
#             print(f"     Targets Hit: {stats['hit_target']}")
#             print(f"     Stop Losses Hit: {stats['hit_sl']}")
#             print(f"     Still Active: {stats['still_active']}")
        
#         return stats
        
#     except Exception as e:
#         print(f"  ⚠️ Error monitoring signals: {e}")
#         return stats


# def extract_latest_candles(df: pd.DataFrame) -> dict:
#     """Extract the latest (most recent) candle data for each symbol."""
#     if df.empty:
#         return {}
    
#     latest_candles = {}
    
#     for symbol in df['symbol'].unique():
#         symbol_df = df[df['symbol'] == symbol].sort_values('timestamp', ascending=False)
#         if not symbol_df.empty:
#             latest = symbol_df.iloc[0]
#             latest_candles[symbol] = {
#                 'high': float(latest['high']),
#                 'low': float(latest['low']),
#                 'close': float(latest['close']),
#                 'timestamp': latest['timestamp']
#             }
    
#     return latest_candles


# # ==================== FETCH LOGIC ====================

# def fetch_ohlcv_direct(symbol: str, timeframe: str, limit: int = None, end_date=None):
#     url = "https://api.binance.us/api/v3/klines"
#     params = {'symbol': symbol, 'interval': timeframe}
    
#     if limit:
#         params['limit'] = limit
#     if end_date:
#         end_dt = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)
#         end_ts = int(end_dt.timestamp() * 1000)
#         params['endTime'] = end_ts
    
#     try:
#         response = requests.get(url, params=params, timeout=15)
#         if response.status_code in [400, 451]:
#             print(f"  ⚠️ Symbol {symbol} not available on Binance.US")
#             return pd.DataFrame()
#         response.raise_for_status()
#         data = response.json()
#         if not data:
#             return pd.DataFrame()

#         df = pd.DataFrame(data, columns=[
#             'timestamp', 'open', 'high', 'low', 'close', 'volume',
#             'close_time', 'quote_volume', 'trades', 'taker_buy_base',
#             'taker_buy_quote', 'ignore'
#         ])
#         df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
#         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
#         df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

#         display_symbol = next((k for k, v in SYMBOL_MAP.items() if v == symbol), symbol)
#         df['symbol'] = display_symbol

#         # Calculate ATR
#         high_low = df['high'] - df['low']
#         high_close = abs(df['high'] - df['close'].shift())
#         low_close = abs(df['low'] - df['close'].shift())
#         true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
#         df['atr'] = true_range.rolling(window=14, min_periods=1).mean()

#         last_date = df['timestamp'].iloc[-1].date() if not df.empty else "none"
#         print(f"  ✅ GOT {len(df)} candles → up to {last_date} | ATR range: {df['atr'].min():.2f} - {df['atr'].max():.2f}")

#         return df

#     except requests.exceptions.Timeout:
#         print(f"  ⏱️ TIMEOUT for {symbol} {timeframe}")
#         return pd.DataFrame()
#     except Exception as e:
#         print(f"  ❌ ERROR {symbol} {timeframe}: {e}")
#         return pd.DataFrame()


# def store_candles_postgresql_batch(df: pd.DataFrame, tf: str):
#     """
#     OPTIMIZED: Batch insert using execute_values (100x faster than row-by-row)
#     """
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

#     try:
#         conn = psycopg2.connect(conn_str)
#         cur = conn.cursor()

#         # Prepare data for batch insert
#         values = []
#         for _, row in df.iterrows():
#             values.append((
#                 row['symbol'],
#                 db_tf,
#                 row['timestamp'],
#                 float(row['open']),
#                 float(row['high']),
#                 float(row['low']),
#                 float(row['close']),
#                 float(row['volume']),
#                 float(row['atr']) if 'atr' in row and pd.notna(row['atr']) else None
#             ))

#         # BATCH INSERT using execute_values (much faster!)
#         sql = """
#         INSERT INTO crypto_candles (symbol, timeframe, timestamp, open, high, low, close, volume, atr)
#         VALUES %s
#         ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
#             open = EXCLUDED.open,
#             high = EXCLUDED.high,
#             low = EXCLUDED.low,
#             close = EXCLUDED.close,
#             volume = EXCLUDED.volume,
#             atr = EXCLUDED.atr
#         """
        
#         psycopg2.extras.execute_values(
#             cur, 
#             sql, 
#             values,
#             template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
#             page_size=1000  # Insert 1000 rows at a time
#         )

#         conn.commit()
#         print(f"   💾 BATCH STORED {len(values)} rows @ {db_tf} (with ATR)")
#         return len(values)

#     except Exception as e:
#         print(f"   🛑 DB ERROR: {e}")
#         return 0
#     finally:
#         if conn:
#             conn.close()


# def main():
#     # === Parse command line arguments ===
#     target_symbols = None
#     target_date = None

#     args = sys.argv[1:]
#     i = 0
#     while i < len(args):
#         arg = args[i]
#         if arg == "--date" and i + 1 < len(args):
#             try:
#                 target_date = datetime.strptime(args[i+1], "%Y-%m-%d").date()
#                 print(f"Date override: Fetching data up to {target_date}")
#                 i += 2
#             except ValueError:
#                 print("Invalid date format. Use YYYY-MM-DD")
#                 sys.exit(1)
#         else:
#             if target_symbols is None:
#                 target_symbols = []
#             target_symbols.append(arg.upper())
#             i += 1

#     # Determine symbols to fetch
#     if target_symbols:
#         valid_symbols = [s for s in target_symbols if s in SYMBOL_MAP]
#         if not valid_symbols:
#             print(f"No valid symbols provided. Available: {', '.join(DESIRED_COINS)}")
#             sys.exit(1)
#         symbols_to_fetch = [SYMBOL_MAP[s] for s in valid_symbols]
#         print(f"Manual mode: Fetching only {valid_symbols}")
#     else:
#         symbols_to_fetch = [SYMBOL_MAP[c] for c in DESIRED_COINS]
#         print("Full mode: Fetching all configured coins")

#     total_tasks = len(symbols_to_fetch) * len(TIMEFRAMES)
#     completed = 0

#     print("=" * 80)
#     print("JOAI DATA HARVESTER — BINANCE.US (OPTIMIZED)")
#     print(f"Symbols: {len(symbols_to_fetch)} | Timeframes: {len(TIMEFRAMES)} | Tasks: {total_tasks}")
#     print(f"Started: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
#     print("=" * 80)

#     all_candles = []
#     start_time = time.time()

#     for binance_symbol in symbols_to_fetch:
#         display_coin = next((k for k, v in SYMBOL_MAP.items() if v == binance_symbol), binance_symbol)
#         coin_name = PRETTY_NAME.get(display_coin, display_coin)
#         print(f"\nFetching {coin_name} → using {binance_symbol}...")

#         for tf, default_limit in TIMEFRAMES.items():
#             completed += 1
#             limit = 1000 if target_date else default_limit
#             print(f"  [{completed}/{total_tasks}] {tf:<3} → ", end="", flush=True)

#             df = fetch_ohlcv_direct(binance_symbol, tf, limit=limit, end_date=target_date)
#             if not df.empty:
#                 store_candles_postgresql_batch(df, tf)  # ← BATCH INSERT
#                 all_candles.append(df)
#             else:
#                 print("NO DATA")
            
#             # REDUCED sleep delay (only 200ms instead of 800ms)
#             time.sleep(0.2)

#     elapsed = time.time() - start_time
#     print(f"\n⏱️ Data fetch completed in {elapsed:.2f} seconds")

#     # === SIGNAL MONITORING ===
#     print("\n" + "=" * 80)
#     print("SIGNAL MONITORING")
#     print("=" * 80)
    
#     if all_candles:
#         combined_df = pd.concat(all_candles, ignore_index=True)
#         latest_candles = extract_latest_candles(combined_df)
        
#         if latest_candles:
#             print(f"Latest candle data available for: {', '.join(latest_candles.keys())}")
            
#             try:
#                 stats = asyncio.run(monitor_and_update_signals(latest_candles))
#                 print(f"\n✅ Signal monitoring complete:")
#                 print(f"   Active signals checked: {stats['total_active']}")
#                 print(f"   Signals updated: {stats['hit_target'] + stats['hit_sl']}")
#             except Exception as e:
#                 print(f"⚠️ Signal monitoring failed: {e}")
#         else:
#             print("No latest candle data available for signal monitoring")
#     else:
#         print("No candle data fetched, skipping signal monitoring")

#     total_elapsed = time.time() - start_time
#     print("\n" + "=" * 80)
#     print("FETCH COMPLETE")
#     print(f"Finished: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
#     print(f"Total execution time: {total_elapsed:.2f} seconds")
#     print("JoAI database updated with fresh Binance.US data")
#     print("Signals monitored and updated in real-time")
#     print("=" * 80)


# if __name__ == "__main__":
#     main()
