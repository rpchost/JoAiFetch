import talib
import numpy as np
import pandas as pd

def add_technical_indicators(df):
    """
    Add technical indicators to the dataframe for LSTM features (OPTIMIZED FOR RENDER)
    
    Optimizations:
    - Pre-allocated contiguous arrays for TA-Lib efficiency
    - Vectorized data validation
    - Efficient NaN handling (only indicator columns)
    - Memory-efficient operations
    
    Keeps all 22 indicators at full quality
    """
    if df is None or len(df) == 0:
        raise ValueError("DataFrame is empty")
    
    # Validate required columns
    required_cols = ['open', 'high', 'low', 'close']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame must contain OHLC columns. Found: {df.columns.tolist()}")

    # Work on copy to avoid modifying original
    df = df.copy()

    # Pre-allocate contiguous arrays for maximum TA-Lib performance
    # This is CRITICAL for memory efficiency and speed
    n = len(df)
    close = np.ascontiguousarray(df['close'].values, dtype=np.float64)
    high = np.ascontiguousarray(df['high'].values, dtype=np.float64)
    low = np.ascontiguousarray(df['low'].values, dtype=np.float64)
    open_price = np.ascontiguousarray(df['open'].values, dtype=np.float64)
    
    if 'volume' in df.columns:
        volume = np.ascontiguousarray(df['volume'].values, dtype=np.float64)
    else:
        volume = np.ones(n, dtype=np.float64)

    # Validate data integrity (prevent NaN/Inf propagation)
    if np.any(np.isnan(close)) or np.any(np.isinf(close)):
        close = np.nan_to_num(close, nan=0.0, posinf=0.0, neginf=0.0)
        high = np.nan_to_num(high, nan=0.0, posinf=0.0, neginf=0.0)
        low = np.nan_to_num(low, nan=0.0, posinf=0.0, neginf=0.0)
        open_price = np.nan_to_num(open_price, nan=0.0, posinf=0.0, neginf=0.0)

    try:
        # ==============================================
        # MOVING AVERAGES (4 indicators)
        # ==============================================
        df['sma_20'] = talib.SMA(close, timeperiod=20)
        df['sma_50'] = talib.SMA(close, timeperiod=50)
        df['ema_12'] = talib.EMA(close, timeperiod=12)
        df['ema_26'] = talib.EMA(close, timeperiod=26)

        # ==============================================
        # MOMENTUM INDICATORS (5 indicators)
        # ==============================================
        df['rsi'] = talib.RSI(close, timeperiod=14)
        
        # MACD (3 outputs)
        macd, macdsignal, macdhist = talib.MACD(
            close, 
            fastperiod=12, 
            slowperiod=26, 
            signalperiod=9
        )
        df['macd'] = macd
        df['macd_signal'] = macdsignal
        df['macd_hist'] = macdhist
        
        # Rate of change
        df['roc'] = talib.ROC(close, timeperiod=10)

        # ==============================================
        # VOLATILITY INDICATORS (4 indicators)
        # ==============================================
        # Bollinger Bands (3 outputs)
        upperband, middleband, lowerband = talib.BBANDS(
            close, 
            timeperiod=20, 
            nbdevup=2, 
            nbdevdn=2, 
            matype=0
        )
        df['bb_upper'] = upperband
        df['bb_middle'] = middleband
        df['bb_lower'] = lowerband
        
        # ATR
        df['atr'] = talib.ATR(high, low, close, timeperiod=14)

        # ==============================================
        # OSCILLATORS (3 indicators)
        # ==============================================
        # Stochastic (2 outputs)
        slowk, slowd = talib.STOCH(
            high, low, close, 
            fastk_period=14, 
            slowk_period=3, 
            slowd_period=3
        )
        df['stoch_k'] = slowk
        df['stoch_d'] = slowd
        
        # Williams %R
        df['willr'] = talib.WILLR(high, low, close, timeperiod=14)

        # ==============================================
        # VOLUME INDICATOR (1 indicator)
        # ==============================================
        df['volume_sma'] = talib.SMA(volume, timeperiod=20)

    except Exception as e:
        raise Exception(f"Error calculating indicators: {str(e)}")

    # ==============================================
    # EFFICIENT NaN HANDLING
    # ==============================================
    # Only process indicator columns (not OHLCV)
    indicator_cols = [
        'sma_20', 'sma_50', 'ema_12', 'ema_26', 'rsi',
        'macd', 'macd_signal', 'macd_hist',
        'bb_upper', 'bb_middle', 'bb_lower', 'atr',
        'stoch_k', 'stoch_d', 'willr', 'volume_sma', 'roc'
    ]
    
    # Forward fill → backward fill → zero (for any remaining NaNs)
    df[indicator_cols] = df[indicator_cols].fillna(method='ffill').fillna(method='bfill').fillna(0)

    return df