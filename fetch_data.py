# fetch_data.py — FLEXIBLE BINANCE.US HARVESTER WITH SIGNAL MONITORING
# Supports: specific symbol, specific date, or full run
# NEW: Real-time signal monitoring - checks if targets/SL hit on every candle fetch

import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
import os
import time
import psycopg2
import sys
from dotenv import load_dotenv
import asyncio
import asyncpg

load_dotenv()

# === CONFIG ===
TIMEFRAMES = {
    #"1m": 1000,
    #"5m": 1000,
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


# ==================== SIGNAL MONITORING LOGIC ====================

async def monitor_and_update_signals(latest_candles: dict):
    """
    Monitor active signals and update them if target or stop loss is hit.
    
    This function runs after fetching new candle data and checks all active signals
    against the latest OHLC prices. If a signal's target or stop loss has been hit,
    it updates the signal status immediately.
    
    Args:
        latest_candles: Dict[symbol -> Dict] containing latest OHLC data
                       Format: {'BTCUSD': {'high': 50000, 'low': 49000, 'close': 49500, ...}, ...}
    
    Returns:
        Dict with statistics about signals updated
    """
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        print("  ⚠️ DATABASE_URL not set, skipping signal monitoring")
        return {'total': 0, 'updated': 0}
    
    if "sslmode" not in conn_str:
        conn_str += "?sslmode=require"
    
    stats = {
        'total_active': 0,
        'hit_target': 0,
        'hit_sl': 0,
        'still_active': 0
    }
    
    try:
        conn = await asyncpg.connect(conn_str)
        
        # Fetch all active signals
        signals = await conn.fetch("""
            SELECT 
                id, symbol, direction, entry_price,
                target_price_1, target_price_2, target_price_3,
                stop_loss, confidence_score, time_generated
            FROM signals
            WHERE status = 'active'
            ORDER BY time_generated DESC
        """)
        
        stats['total_active'] = len(signals)
        
        if not signals:
            await conn.close()
            return stats
        
        print(f"\n  📊 Monitoring {len(signals)} active signals...")
        
        for signal in signals:
            symbol = signal['symbol']
            
            # Skip if we don't have latest candle data for this symbol
            if symbol not in latest_candles:
                continue
            
            candle = latest_candles[symbol]
            high = candle['high']
            low = candle['low']
            close = candle['close']
            
            direction = signal['direction']
            target_1 = signal['target_price_1']
            target_2 = signal['target_price_2']
            target_3 = signal['target_price_3']
            sl = signal['stop_loss']
            
            # Determine if signal should be updated
            new_status = None
            hit_price = None
            
            if direction == 'LONG':
                # Check if stop loss was hit (price went down)
                if sl is not None and low <= sl:
                    new_status = 'hit_sl'
                    hit_price = sl
                # Check if any target was hit (price went up)
                elif target_3 is not None and high >= target_3:
                    new_status = 'hit_target'
                    hit_price = target_3
                elif target_2 is not None and high >= target_2:
                    new_status = 'hit_target'
                    hit_price = target_2
                elif target_1 is not None and high >= target_1:
                    new_status = 'hit_target'
                    hit_price = target_1
            
            elif direction == 'SHORT':
                # Check if stop loss was hit (price went up)
                if sl is not None and high >= sl:
                    new_status = 'hit_sl'
                    hit_price = sl
                # Check if any target was hit (price went down)
                elif target_3 is not None and low <= target_3:
                    new_status = 'hit_target'
                    hit_price = target_3
                elif target_2 is not None and low <= target_2:
                    new_status = 'hit_target'
                    hit_price = target_2
                elif target_1 is not None and low <= target_1:
                    new_status = 'hit_target'
                    hit_price = target_1
            
            # Update signal if status changed
            if new_status:
                await conn.execute("""
                    UPDATE signals
                    SET 
                        status = $1,
                        updated_at = NOW()
                    WHERE id = $2
                """, new_status, signal['id'])
                
                if new_status == 'hit_target':
                    stats['hit_target'] += 1
                    print(f"    ✅ Signal #{signal['id']} ({symbol} {direction}) HIT TARGET @ ${hit_price:,.2f}")
                elif new_status == 'hit_sl':
                    stats['hit_sl'] += 1
                    print(f"    ❌ Signal #{signal['id']} ({symbol} {direction}) HIT STOP LOSS @ ${hit_price:,.2f}")
            else:
                stats['still_active'] += 1
        
        await conn.close()
        
        # Print summary if any signals were updated
        if stats['hit_target'] > 0 or stats['hit_sl'] > 0:
            print(f"  📈 Signal Update Summary:")
            print(f"     Targets Hit: {stats['hit_target']}")
            print(f"     Stop Losses Hit: {stats['hit_sl']}")
            print(f"     Still Active: {stats['still_active']}")
        
        return stats
        
    except Exception as e:
        print(f"  ⚠️ Error monitoring signals: {e}")
        return stats


def extract_latest_candles(df: pd.DataFrame) -> dict:
    """
    Extract the latest (most recent) candle data for each symbol.
    
    Args:
        df: DataFrame with OHLC data
    
    Returns:
        Dict[symbol -> Dict] with latest high, low, close prices
    """
    if df.empty:
        return {}
    
    latest_candles = {}
    
    for symbol in df['symbol'].unique():
        symbol_df = df[df['symbol'] == symbol].sort_values('timestamp', ascending=False)
        if not symbol_df.empty:
            latest = symbol_df.iloc[0]
            latest_candles[symbol] = {
                'high': float(latest['high']),
                'low': float(latest['low']),
                'close': float(latest['close']),
                'timestamp': latest['timestamp']
            }
    
    return latest_candles


# ==================== ORIGINAL FETCH LOGIC ====================

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

    # Store all fetched data for signal monitoring
    all_candles = []

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
                all_candles.append(df)
            else:
                print("NO DATA")
            time.sleep(0.8)

    # === SIGNAL MONITORING (NEW) ===
    print("\n" + "=" * 80)
    print("SIGNAL MONITORING")
    print("=" * 80)
    
    if all_candles:
        # Combine all candles and extract latest data
        combined_df = pd.concat(all_candles, ignore_index=True)
        latest_candles = extract_latest_candles(combined_df)
        
        if latest_candles:
            print(f"Latest candle data available for: {', '.join(latest_candles.keys())}")
            
            # Run signal monitoring asynchronously
            try:
                stats = asyncio.run(monitor_and_update_signals(latest_candles))
                print(f"\n✅ Signal monitoring complete:")
                print(f"   Active signals checked: {stats['total_active']}")
                print(f"   Signals updated: {stats['hit_target'] + stats['hit_sl']}")
            except Exception as e:
                print(f"⚠️ Signal monitoring failed: {e}")
        else:
            print("No latest candle data available for signal monitoring")
    else:
        print("No candle data fetched, skipping signal monitoring")

    print("\n" + "=" * 80)
    print("FETCH COMPLETE")
    print(f"Finished: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
    print("JoAI database updated with fresh Binance.US data")
    print("Signals monitored and updated in real-time")
    print("=" * 80)


if __name__ == "__main__":
    main()

# # fetch_data.py — FLEXIBLE BINANCE.US HARVESTER — JAN 2026
# # Supports: specific symbol, specific date, or full run

# import pandas as pd
# import requests
# from datetime import datetime, timedelta, timezone
# import os
# import time
# import psycopg2
# import sys
# from dotenv import load_dotenv

# load_dotenv()

# # === CONFIG ===
# TIMEFRAMES = {
#     "1m": 1000,
#     "5m": 1000,
#     "1h": 1000,
#     "4h": 1000,
#     "1d": 1000
# }

# DESIRED_COINS = [
#     "BTCUSD", "ETHUSD"
#     # , "SOLUSD", "ADAUSD", "BNBUSD",
#     # "XRPUSD", "DOGEUSD", "SHIBUSD", "PEPEUSD",
#     # "LINKUSD", "AVAXUSD", "TONUSD"
# ]

# SYMBOL_MAP = {
#     "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT"
#     # , "SOLUSD": "SOLUSDT",
#     # "ADAUSD": "ADAUSDT", "BNBUSD": "BNBUSDT", "XRPUSD": "XRPUSDT",
#     # "DOGEUSD": "DOGEUSDT", "SHIBUSD": "SHIBUSDT", "PEPEUSD": "PEPEUSDT",
#     # "LINKUSD": "LINKUSDT", "AVAXUSD": "AVAXUSDT", "TONUSD": "TONUSDT"
# }

# PRETTY_NAME = {
#     "BTCUSD": "Bitcoin",
#     "ETHUSD": "Ethereum",
#     # "SOLUSD": "Solana",
#     # "ADAUSD": "Cardano",
#     # "BNBUSD": "Binance Coin",
#     # "XRPUSD": "Ripple",
#     # "DOGEUSD": "Dogecoin",
#     # "SHIBUSD": "Shiba Inu",
#     # "PEPEUSD": "Pepe",
#     # "LINKUSD": "Chainlink",
#     # "AVAXUSD": "Avalanche",
#     # "TONUSD": "Toncoin"
# }

# def fetch_ohlcv_direct(symbol: str, timeframe: str, limit: int = None, end_date=None):
#     url = "https://api.binance.us/api/v3/klines"
#     params = {'symbol': symbol, 'interval': timeframe}
    
#     if limit:
#         params['limit'] = limit
#     if end_date:
#         # End time = end of specified day (23:59:59.999 UTC)
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

#         # Map back to desired XXXUSD symbol
#         display_symbol = next((k for k, v in SYMBOL_MAP.items() if v == symbol), symbol)
#         df['symbol'] = display_symbol

#         # === CALCULATE ATR (14-period standard) ===
#         high_low = df['high'] - df['low']
#         high_close = abs(df['high'] - df['close'].shift())
#         low_close = abs(df['low'] - df['close'].shift())
#         true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
#         df['atr'] = true_range.rolling(window=14, min_periods=1).mean()  # min_periods=1 for early rows

#         last_date = df['timestamp'].iloc[-1].date() if not df.empty else "none"
#         print(f"  ✅ GOT {len(df)} candles → up to {last_date} | ATR range: {df['atr'].min():.2f} - {df['atr'].max():.2f}")

#         return df

#     except requests.exceptions.Timeout:
#         print(f"  ⏱️ TIMEOUT for {symbol} {timeframe}")
#         return pd.DataFrame()
#     except Exception as e:
#         print(f"  ❌ ERROR {symbol} {timeframe}: {e}")
#         return pd.DataFrame()

# def store_candles_postgresql(df: pd.DataFrame, tf: str):
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

#         sql = """
#         INSERT INTO crypto_candles (symbol, timeframe, timestamp, open, high, low, close, volume, atr)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
#             open = EXCLUDED.open,
#             high = EXCLUDED.high,
#             low = EXCLUDED.low,
#             close = EXCLUDED.close,
#             volume = EXCLUDED.volume,
#             atr = EXCLUDED.atr
#         """

#         inserted = 0
#         for _, row in df.iterrows():
#             cur.execute(sql, (
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
#             if cur.rowcount > 0:
#                 inserted += 1

#         conn.commit()
#         print(f"   💾 STORED/UPDATED {inserted} rows @ {db_tf} (with ATR)")
#         return inserted

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
#     print("JOAI DATA HARVESTER — BINANCE.US (USDT PAIRS → USD SYMBOLS)")
#     print(f"Symbols: {len(symbols_to_fetch)} | Timeframes: {len(TIMEFRAMES)} | Tasks: {total_tasks}")
#     print(f"Started: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
#     print("=" * 80)

#     for binance_symbol in symbols_to_fetch:
#         # Safe lookup
#         display_coin = next((k for k, v in SYMBOL_MAP.items() if v == binance_symbol), binance_symbol)
#         coin_name = PRETTY_NAME.get(display_coin, display_coin)
#         print(f"\nFetching {coin_name} → using {binance_symbol}...")

#         for tf, default_limit in TIMEFRAMES.items():
#             completed += 1
#             limit = 1000 if target_date else default_limit
#             print(f"  [{completed}/{total_tasks}] {tf:<3} → ", end="", flush=True)

#             df = fetch_ohlcv_direct(binance_symbol, tf, limit=limit, end_date=target_date)
#             if not df.empty:
#                 store_candles_postgresql(df, tf)
#             else:
#                 print("NO DATA")
#             time.sleep(0.8)

#     print("=" * 80)
#     print("FETCH COMPLETE")
#     print(f"Finished: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}")
#     print("JoAI database updated with fresh Binance.US data")
#     print("=" * 80)

# if __name__ == "__main__":
#     main()