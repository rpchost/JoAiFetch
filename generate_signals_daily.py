# generate_signals.py - FIXED VERSION
# Key changes:
# 1. Direction is determined by comparing predicted_close to CURRENT price (not predicted_open)
# 2. Added threshold to avoid noise
# 3. Better logging for debugging

"""
Signal Generation System for Crypto Trading Predictions (FIXED)

Changes from original:
- Direction now compares predicted_close to CURRENT price (not predicted_open)
- Added 0.5% threshold to filter out noise
- Improved logging for signal rejection reasons
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from dataclasses import dataclass

import asyncpg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== ENUMS ====================

class SignalDirection(Enum):
    """Signal direction types"""
    LONG = "LONG"
    SHORT = "SHORT"
    HOLD = "HOLD"


class SignalStatus(Enum):
    """Signal status types"""
    ACTIVE = "active"
    HIT_TARGET = "hit_target"
    HIT_SL = "hit_sl"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    CLOSED = "closed"


# ==================== DATA CLASSES ====================

@dataclass
class SignalConfig:
    """Configuration for signal generation and risk management"""
    
    # Risk management - ATR multipliers
    atr_multiplier_sl: float = 1.5
    atr_multiplier_tp1: float = 2.0
    atr_multiplier_tp2: float = 3.0
    atr_multiplier_tp3: float = 4.0
    
    # Confidence calculation
    base_confidence: float = 50.0
    move_pct_multiplier: float = 10.0
    max_confidence: float = 100.0
    
    # Direction threshold (0.5% to avoid noise)
    direction_threshold_pct: float = 0.005  # 0.5%
    
    # Signal expiry
    default_expiry_hours: int = 24
    
    # Database connection pool
    pool_min_size: int = 2
    pool_max_size: int = 10


@dataclass
class PredictionData:
    """Container for prediction data"""
    prediction_id: int
    symbol: str
    timeframe: str
    predicted_open: float
    predicted_high: float
    predicted_low: float
    predicted_close: float
    for_date: datetime
    custom_indicator_id: Optional[int] = None
    user_id: Optional[int] = None
    is_personal: bool = False


@dataclass
class MarketData:
    """Container for current market data"""
    symbol: str
    current_close: float
    atr: float
    timestamp: datetime


@dataclass
class SignalData:
    """Container for generated signal data"""
    symbol: str
    direction: SignalDirection
    entry_price: float
    target_price_1: float
    target_price_2: Optional[float] = None
    target_price_3: Optional[float] = None
    stop_loss: Optional[float] = None
    confidence_score: float = 50.0
    time_expiry: Optional[datetime] = None
    custom_indicator_id: Optional[int] = None
    daily_prediction_id_1hour: Optional[int] = None
    daily_prediction_id_1day: Optional[int] = None
    personal_prediction_id_1hour: Optional[int] = None
    personal_prediction_id_1day: Optional[int] = None


# ==================== MAIN CLASS ====================

class SignalGenerator:
    """
    Async signal generation and management system for crypto trading.
    
    FIXED: Direction is now determined by comparing predicted_close to CURRENT price
    (not predicted_open). This gives proper trading signals.
    """
    
    def __init__(
        self,
        database_url: Optional[str] = None,
        config: Optional[SignalConfig] = None
    ):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables")
        
        if "sslmode" not in self.database_url:
            self.database_url += "?sslmode=require"
        
        self.config = config or SignalConfig()
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self) -> None:
        """Initialize database connection pool"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    self.database_url,
                    min_size=self.config.pool_min_size,
                    max_size=self.config.pool_max_size,
                    command_timeout=60
                )
                logger.info("Database connection pool initialized")
            except Exception as e:
                logger.error(f"Failed to initialize database pool: {e}")
                raise
    
    async def close(self) -> None:
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    # ==================== CORE SIGNAL GENERATION ====================
    
    async def generate(
        self,
        prediction_type: str = 'global',
        user_id: Optional[int] = None,
        symbols: Optional[List[str]] = None,
        for_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Main signal generation method"""
        stats = {
            'total_symbols': 0,
            'signals_generated': 0,
            'signals_skipped': 0,
            'errors': 0,
            'details': []
        }
        
        try:
            if prediction_type not in ['global', 'personal']:
                raise ValueError(f"Invalid prediction_type: {prediction_type}")
            
            if prediction_type == 'personal' and not user_id:
                raise ValueError("user_id required for personal predictions")
            
            # Fetch predictions
            if prediction_type == 'global':
                predictions_by_symbol = await self._fetch_global_predictions_grouped(symbols, for_date)
            else:
                predictions_by_symbol = await self._fetch_personal_predictions_grouped(user_id, symbols, for_date)
            
            stats['total_symbols'] = len(predictions_by_symbol)
            logger.info(f"Processing {stats['total_symbols']} symbols for {prediction_type} signals")
            
            # Generate signals concurrently
            tasks = [
                self._generate_signal_for_symbol(symbol, predictions, prediction_type == 'personal')
                for symbol, predictions in predictions_by_symbol.items()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for symbol, result in zip(predictions_by_symbol.keys(), results):
                if isinstance(result, Exception):
                    stats['errors'] += 1
                    stats['details'].append({
                        'symbol': symbol,
                        'status': 'error',
                        'error': str(result)
                    })
                    logger.error(f"Error generating signal for {symbol}: {result}")
                elif result['success']:
                    stats['signals_generated'] += 1
                    stats['details'].append({
                        'symbol': symbol,
                        'status': 'generated',
                        'signal_id': result.get('signal_id'),
                        'direction': result.get('direction'),
                        'confidence': result.get('confidence')
                    })
                else:
                    stats['signals_skipped'] += 1
                    stats['details'].append({
                        'symbol': symbol,
                        'status': 'skipped',
                        'reason': result.get('reason')
                    })
            
            logger.info(
                f"Signal generation complete: {stats['signals_generated']} generated, "
                f"{stats['signals_skipped']} skipped, {stats['errors']} errors"
            )
            
        except Exception as e:
            logger.error(f"Critical error in generate(): {e}")
            raise
        
        return stats
    
    # ==================== PREDICTION FETCHING ====================
    
    async def _fetch_global_predictions_grouped(
        self,
        symbols: Optional[List[str]] = None,
        for_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, PredictionData]]:
        """Fetch global predictions grouped by symbol and timeframe"""
        query = """
            SELECT 
                id, symbol, timeframe,
                predicted_open, predicted_high, predicted_low, predicted_close,
                for_date
            FROM daily_predictions
            WHERE resolved_at IS NULL
              AND timeframe IN ('1 hour', '1 day')
        """
        params = []
        
        if symbols:
            query += f" AND symbol = ANY($1)"
            params.append(symbols)
        
        if for_date:
            param_num = len(params) + 1
            query += f" AND for_date = ${param_num}"
            params.append(for_date)
        else:
            tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
            param_num = len(params) + 1
            query += f" AND for_date = ${param_num}"
            params.append(tomorrow)
        
        query += " ORDER BY symbol, timeframe"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        return self._group_predictions_by_symbol(rows, is_personal=False)
    
    async def _fetch_personal_predictions_grouped(
        self,
        user_id: int,
        symbols: Optional[List[str]] = None,
        for_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, PredictionData]]:
        """Fetch personal predictions grouped by symbol and timeframe"""
        query = """
            SELECT 
                id, user_id, symbol, timeframe,
                predicted_open, predicted_high, predicted_low, predicted_close,
                for_date, custom_indicator_id
            FROM personal_daily_predictions
            WHERE resolved_at IS NULL
              AND user_id = $1
              AND timeframe IN ('1 hour', '1 day')
        """
        params = [user_id]
        
        if symbols:
            query += f" AND symbol = ANY($2)"
            params.append(symbols)
        
        if for_date:
            param_num = len(params) + 1
            query += f" AND for_date = ${param_num}"
            params.append(for_date)
        else:
            tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
            param_num = len(params) + 1
            query += f" AND for_date = ${param_num}"
            params.append(tomorrow)
        
        query += " ORDER BY symbol, timeframe"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        return self._group_predictions_by_symbol(rows, is_personal=True)
    
    def _group_predictions_by_symbol(
        self,
        rows: List[asyncpg.Record],
        is_personal: bool
    ) -> Dict[str, Dict[str, PredictionData]]:
        """Group prediction rows by symbol and timeframe"""
        predictions_by_symbol = {}
        
        for row in rows:
            if is_personal:
                pred = PredictionData(
                    prediction_id=row['id'],
                    user_id=row['user_id'],
                    symbol=row['symbol'],
                    timeframe=row['timeframe'],
                    predicted_open=float(row['predicted_open'] or 0),
                    predicted_high=float(row['predicted_high'] or 0),
                    predicted_low=float(row['predicted_low'] or 0),
                    predicted_close=float(row['predicted_close'] or 0),
                    for_date=row['for_date'],
                    custom_indicator_id=row['custom_indicator_id'],
                    is_personal=True
                )
            else:
                pred = PredictionData(
                    prediction_id=row['id'],
                    symbol=row['symbol'],
                    timeframe=row['timeframe'],
                    predicted_open=float(row['predicted_open'] or 0),
                    predicted_high=float(row['predicted_high'] or 0),
                    predicted_low=float(row['predicted_low'] or 0),
                    predicted_close=float(row['predicted_close'] or 0),
                    for_date=row['for_date'],
                    is_personal=False
                )
            
            if pred.symbol not in predictions_by_symbol:
                predictions_by_symbol[pred.symbol] = {}
            
            predictions_by_symbol[pred.symbol][pred.timeframe] = pred
        
        return predictions_by_symbol
    
    # ==================== MARKET DATA ====================
    
    async def _get_market_data(self, symbol: str) -> Optional[MarketData]:
        """Fetch latest market data for a symbol"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT close, atr, timestamp
                FROM crypto_candles
                WHERE symbol = $1 
                  AND timeframe = '1day'
                ORDER BY timestamp DESC
                LIMIT 1
            """, symbol)
        
        if not row or row['close'] is None or row['atr'] is None or row['atr'] <= 0:
            logger.warning(f"Market data unavailable or invalid for {symbol}")
            return None
        
        return MarketData(
            symbol=symbol,
            current_close=float(row['close']),
            atr=float(row['atr']),
            timestamp=row['timestamp']
        )
    
    # ==================== SIGNAL CALCULATION (FIXED) ====================
    
    def _determine_direction(
        self, 
        pred: PredictionData,
        current_price: float
    ) -> SignalDirection:
        """
        FIXED: Determine direction by comparing predicted_close to CURRENT price
        
        This is the KEY FIX - we now compare against current market price,
        not against predicted_open.
        """
        threshold = current_price * self.config.direction_threshold_pct
        
        if pred.predicted_close > current_price + threshold:
            return SignalDirection.LONG
        elif pred.predicted_close < current_price - threshold:
            return SignalDirection.SHORT
        else:
            return SignalDirection.HOLD
    
    def _check_timeframe_agreement(
        self,
        predictions: Dict[str, PredictionData],
        current_price: float
    ) -> Tuple[bool, Optional[SignalDirection], str]:
        """
        FIXED: Check if both timeframes agree relative to CURRENT price
        
        Returns: (agreement, direction, debug_info)
        """
        if '1 hour' not in predictions or '1 day' not in predictions:
            timeframes = list(predictions.keys())
            return False, None, f'Missing timeframes (have: {timeframes})'
        
        pred_1hour = predictions['1 hour']
        pred_1day = predictions['1 day']
        
        direction_1hour = self._determine_direction(pred_1hour, current_price)
        direction_1day = self._determine_direction(pred_1day, current_price)
        
        debug_info = (
            f"1h: ${pred_1hour.predicted_close:.2f} → {direction_1hour.value}, "
            f"1d: ${pred_1day.predicted_close:.2f} → {direction_1day.value} "
            f"(current: ${current_price:.2f})"
        )
        
        # Both must agree and neither can be HOLD
        if direction_1hour == direction_1day and direction_1hour != SignalDirection.HOLD:
            return True, direction_1hour, debug_info
        
        return False, None, debug_info
    
    def _calculate_signal(
        self,
        symbol: str,
        direction: SignalDirection,
        predictions: Dict[str, PredictionData],
        market: MarketData,
        is_personal: bool
    ) -> Optional[SignalData]:
        """Calculate signal parameters"""
        pred_1day = predictions['1 day']
        pred_1hour = predictions.get('1 hour')
        
        current_close = market.current_close
        atr = market.atr
        
        entry_price = current_close
        target_price_1 = pred_1day.predicted_close
        
        # Calculate targets and stop loss
        if direction == SignalDirection.LONG:
            if target_price_1 <= current_close:
                return None  # Sanity check: target must be above entry for LONG
            
            target_price_2 = entry_price + (atr * self.config.atr_multiplier_tp2)
            target_price_3 = entry_price + (atr * self.config.atr_multiplier_tp3)
            stop_loss = entry_price - (atr * self.config.atr_multiplier_sl)
        else:  # SHORT
            if target_price_1 >= current_close:
                return None  # Sanity check: target must be below entry for SHORT
            
            target_price_2 = entry_price - (atr * self.config.atr_multiplier_tp2)
            target_price_3 = entry_price - (atr * self.config.atr_multiplier_tp3)
            stop_loss = entry_price + (atr * self.config.atr_multiplier_sl)
        
        # Confidence score
        move_pct = abs(pred_1day.predicted_close - current_close) / current_close * 100
        confidence = min(
            self.config.max_confidence,
            self.config.base_confidence + (move_pct * self.config.move_pct_multiplier)
        )
        
        time_expiry = datetime.now(timezone.utc) + timedelta(hours=self.config.default_expiry_hours)
        
        signal = SignalData(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            target_price_1=target_price_1,
            target_price_2=target_price_2,
            target_price_3=target_price_3,
            stop_loss=stop_loss,
            confidence_score=round(confidence, 2),
            time_expiry=time_expiry,
            custom_indicator_id=pred_1day.custom_indicator_id if is_personal else None
        )
        
        # Link prediction IDs
        if is_personal:
            signal.personal_prediction_id_1day = pred_1day.prediction_id
            signal.personal_prediction_id_1hour = pred_1hour.prediction_id if pred_1hour else None
        else:
            signal.daily_prediction_id_1day = pred_1day.prediction_id
            signal.daily_prediction_id_1hour = pred_1hour.prediction_id if pred_1hour else None
        
        return signal
    
    # ==================== DATABASE OPERATIONS ====================
    
    async def _save_signal(self, signal: SignalData) -> int:
        """Save signal to database"""
        insert_sql = """
            INSERT INTO signals (
                custom_indicator_id,
                symbol, direction, entry_price,
                target_price_1, target_price_2, target_price_3,
                stop_loss, confidence_score,
                time_generated, time_expiry,
                status,
                daily_prediction_id, personal_prediction_id
            ) VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9,
                NOW(), $10,
                $11,
                $12, $13
            )
            RETURNING id
        """
        
        async with self.pool.acquire() as conn:
            signal_id = await conn.fetchval(
                insert_sql,
                signal.custom_indicator_id,
                signal.symbol,
                signal.direction.value,
                signal.entry_price,
                signal.target_price_1,
                signal.target_price_2,
                signal.target_price_3,
                signal.stop_loss,
                signal.confidence_score,
                signal.time_expiry,
                SignalStatus.ACTIVE.value,
                signal.daily_prediction_id_1day,
                signal.personal_prediction_id_1day
            )
        
        return signal_id
    
    async def _check_existing_signal(
        self,
        symbol: str,
        predictions: Dict[str, PredictionData],
        is_personal: bool
    ) -> bool:
        """Check if signal already exists"""
        pred_1day = predictions.get('1 day')
        if not pred_1day:
            return False
        
        async with self.pool.acquire() as conn:
            if is_personal:
                exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM signals
                        WHERE personal_prediction_id = $1 AND symbol = $2
                    )
                """, pred_1day.prediction_id, symbol)
            else:
                exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM signals
                        WHERE daily_prediction_id = $1 AND symbol = $2
                    )
                """, pred_1day.prediction_id, symbol)
        
        return exists
    
    # ==================== WORKFLOW METHODS ====================
    
    async def _generate_signal_for_symbol(
        self,
        symbol: str,
        predictions: Dict[str, PredictionData],
        is_personal: bool
    ) -> Dict[str, Any]:
        """Complete workflow to generate signal for one symbol"""
        
        # Check if signal already exists
        if await self._check_existing_signal(symbol, predictions, is_personal):
            return {
                'success': False,
                'reason': 'Signal already exists for this prediction'
            }
        
        # Get market data
        market = await self._get_market_data(symbol)
        if not market:
            return {'success': False, 'reason': 'Market data not available'}
        
        # Check timeframe agreement (FIXED - now passes current price)
        agreement, direction, debug_info = self._check_timeframe_agreement(
            predictions, 
            market.current_close
        )
        
        if not agreement:
            logger.debug(f"{symbol}: Timeframes disagree - {debug_info}")
            return {'success': False, 'reason': f'Timeframes disagree ({debug_info})'}
        
        # Calculate signal
        signal = self._calculate_signal(symbol, direction, predictions, market, is_personal)
        if signal is None:
            return {
                'success': False, 
                'reason': 'Signal validation failed (target price incompatible with direction)'
            }
        
        # Save signal
        try:
            signal_id = await self._save_signal(signal)
            logger.info(
                f"✅ Generated signal for {symbol}: {signal.direction.value} @ "
                f"${signal.entry_price:.2f} → ${signal.target_price_1:.2f} "
                f"(confidence: {signal.confidence_score}%)"
            )
            return {
                'success': True,
                'signal_id': signal_id,
                'direction': signal.direction.value,
                'confidence': signal.confidence_score,
                'entry_price': signal.entry_price
            }
        except Exception as e:
            logger.error(f"Database error saving signal for {symbol}: {e}")
            return {'success': False, 'reason': f'Database error: {str(e)}'}


# ==================== STANDALONE EXECUTION ====================

async def main():
    """Main execution function"""
    print("=" * 80)
    print("SIGNAL GENERATOR (FIXED VERSION)")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    
    # Generate global signals
    print("\nGenerating GLOBAL signals...")
    async with SignalGenerator() as generator:
        stats = await generator.generate(
            prediction_type='global',
            symbols=['BTCUSD', 'ETHUSD']
        )
    
    print(f"\nGlobal Results:")
    print(f"  Total symbols: {stats['total_symbols']}")
    print(f"  Signals generated: {stats['signals_generated']}")
    print(f"  Signals skipped: {stats['signals_skipped']}")
    print(f"  Errors: {stats['errors']}")
    
    if stats['details']:
        print("\nDetails:")
        for detail in stats['details']:
            if detail['status'] == 'generated':
                print(f"  ✓ {detail['symbol']}: {detail['direction']} (Confidence: {detail['confidence']}%)")
            elif detail['status'] == 'skipped':
                print(f"  - {detail['symbol']}: Skipped ({detail['reason']})")
            elif detail['status'] == 'error':
                print(f"  ✗ {detail['symbol']}: Error ({detail['error']})")
    
    print("\n" + "=" * 80)
    print("Signal generation complete")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())