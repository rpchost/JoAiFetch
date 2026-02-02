"""
Signal Generation System for Crypto Trading Predictions

This module generates trading signals based on LSTM predictions from both global
and personalized models. Signals are only generated when BOTH 1-hour and 1-day
predictions agree on direction (bullish or bearish).

Key Features:
- Async/await for improved performance
- Connection pooling for database efficiency
- Comprehensive error handling and logging
- Support for both global and personal predictions
- Configurable risk management parameters

Usage:
    # Generate global signals
    async with SignalGenerator() as generator:
        stats = await generator.generate(prediction_type='global')
    
    # Generate personal signals for a user
    async with SignalGenerator() as generator:
        stats = await generator.generate(prediction_type='personal', user_id=5)
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager

import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
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
    atr_multiplier_sl: float = 1.5      # Stop loss
    atr_multiplier_tp1: float = 2.0     # Target 1
    atr_multiplier_tp2: float = 3.0     # Target 2
    atr_multiplier_tp3: float = 4.0     # Target 3
    
    # Confidence calculation
    base_confidence: float = 50.0
    move_pct_multiplier: float = 10.0
    max_confidence: float = 100.0
    
    # Direction thresholds
    direction_threshold_multiplier: float = 0.3
    
    # Signal expiry
    default_expiry_hours: int = 24
    
    # Timeframe agreement
    require_both_timeframes: bool = True
    
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
    
    This class generates trading signals based on predictions from both global
    and personalized models. Signals are only generated when BOTH 1-hour and
    1-day predictions agree on the direction.
    
    Logic Flow:
    1. Fetch predictions for tomorrow's date
    2. Group predictions by symbol
    3. Check if BOTH 1hour and 1day timeframes exist for the symbol
    4. Compare predicted_close vs predicted_open for each timeframe
    5. If BOTH timeframes agree (both bullish or both bearish), generate signal
    6. Calculate entry, targets, and stop loss using current market data
    """
    
    def __init__(
        self,
        database_url: Optional[str] = None,
        config: Optional[SignalConfig] = None
    ):
        """
        Initialize the Signal Generator.
        
        Args:
            database_url: PostgreSQL connection string (defaults to env variable)
            config: SignalConfig instance for customization
        """
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables")
        
        # Ensure SSL mode is set
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
        """Async context manager entry"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    # ==================== CORE SIGNAL GENERATION ====================
    
    async def generate(
        self,
        prediction_type: str = 'global',
        user_id: Optional[int] = None,
        symbols: Optional[List[str]] = None,
        for_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Main signal generation method.
        
        Args:
            prediction_type: 'global' or 'personal'
            user_id: User ID for personal predictions (required if prediction_type='personal')
            symbols: Filter by specific symbols (e.g., ['BTCUSD', 'ETHUSD'])
            for_date: Target date for predictions (defaults to tomorrow)
        
        Returns:
            Dict with statistics: {
                'total_symbols': int,
                'signals_generated': int,
                'signals_skipped': int,
                'errors': int,
                'details': List[Dict]
            }
        """
        stats = {
            'total_symbols': 0,
            'signals_generated': 0,
            'signals_skipped': 0,
            'errors': 0,
            'details': []
        }
        
        try:
            # Validate inputs
            if prediction_type not in ['global', 'personal']:
                raise ValueError(f"Invalid prediction_type: {prediction_type}. Use 'global' or 'personal'")
            
            if prediction_type == 'personal' and not user_id:
                raise ValueError("user_id is required for personal prediction type")
            
            # Fetch predictions based on type
            if prediction_type == 'global':
                predictions_by_symbol = await self._fetch_global_predictions_grouped(symbols, for_date)
            else:
                predictions_by_symbol = await self._fetch_personal_predictions_grouped(user_id, symbols, for_date)
            
            stats['total_symbols'] = len(predictions_by_symbol)
            logger.info(f"Processing {stats['total_symbols']} symbols for {prediction_type} signals")
            
            # Generate signals for each symbol concurrently
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
    
    async def resolve(self, signal_id: int, actual_close: float) -> Dict[str, Any]:
        """
        Resolve a single signal based on actual close price.
        
        Args:
            signal_id: ID of the signal to resolve
            actual_close: The real closing price on expiry date
        
        Returns:
            Dict with resolution result
        """
        async with self.pool.acquire() as conn:
            try:
                # Fetch signal details
                row = await conn.fetchrow("""
                    SELECT 
                        id, symbol, direction, entry_price,
                        target_price_1, stop_loss, status,
                        time_expiry
                    FROM signals
                    WHERE id = $1
                """, signal_id)
                
                if not row:
                    return {'success': False, 'reason': f'Signal ID {signal_id} not found'}
                
                # Skip if already resolved
                if row['status'] in ['hit_target', 'hit_sl', 'expired', 'cancelled', 'closed']:
                    return {
                        'success': False,
                        'reason': f"Signal already resolved as {row['status']}"
                    }
                
                # Determine final status
                final_status = await self._determine_signal_status(
                    row['direction'],
                    actual_close,
                    row['entry_price'],
                    row['target_price_1'],
                    row['stop_loss']
                )
                
                # Update signal
                await conn.execute("""
                    UPDATE signals
                    SET 
                        status = $1,
                        updated_at = NOW()
                    WHERE id = $2
                """, final_status, signal_id)
                
                logger.info(
                    f"Signal {signal_id} ({row['symbol']}) resolved: {final_status} | "
                    f"Actual close: {actual_close:.2f}"
                )
                
                return {
                    'success': True,
                    'status': final_status,
                    'signal_id': signal_id,
                    'symbol': row['symbol'],
                    'actual_close': actual_close
                }
            
            except Exception as e:
                logger.error(f"Error resolving signal {signal_id}: {e}")
                return {'success': False, 'reason': str(e)}
    
    # ==================== PREDICTION FETCHING ====================
    
    async def _fetch_global_predictions_grouped(
        self,
        symbols: Optional[List[str]] = None,
        for_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, PredictionData]]:
        """
        Fetch global predictions grouped by symbol and timeframe.
        
        Returns:
            Dict[symbol -> Dict[timeframe -> PredictionData]]
        """
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
            # Default to tomorrow's predictions
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
        """
        Fetch personal predictions grouped by symbol and timeframe.
        
        Returns:
            Dict[symbol -> Dict[timeframe -> PredictionData]]
        """
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
            # Default to tomorrow's predictions
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
        """
        Group prediction rows by symbol and timeframe.
        
        Args:
            rows: Database rows
            is_personal: Whether these are personal predictions
        
        Returns:
            Dict[symbol -> Dict[timeframe -> PredictionData]]
        """
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
        """
        Fetch latest market data for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSD')
        
        Returns:
            MarketData object or None if not available
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT close, atr, timestamp
                FROM crypto_candles
                WHERE symbol = $1 
                  AND timeframe = '1day'
                ORDER BY timestamp DESC
                LIMIT 1
            """, symbol)
        
        if not row:
            logger.warning(f"No market data found for {symbol}")
            return None
        
        # Validate data
        if row['close'] is None:
            logger.warning(f"Latest close is NULL for {symbol}")
            return None
        
        if row['atr'] is None or row['atr'] <= 0:
            logger.warning(f"ATR is missing or invalid for {symbol}")
            return None
        
        return MarketData(
            symbol=symbol,
            current_close=float(row['close']),
            atr=float(row['atr']),
            timestamp=row['timestamp']
        )
    
    # ==================== SIGNAL CALCULATION ====================
    
    def _determine_direction(self, pred: PredictionData) -> SignalDirection:
        """
        Determine direction from a single prediction.
        
        Args:
            pred: PredictionData object
        
        Returns:
            SignalDirection (LONG, SHORT, or HOLD)
        """
        if pred.predicted_close > pred.predicted_open:
            return SignalDirection.LONG
        elif pred.predicted_close < pred.predicted_open:
            return SignalDirection.SHORT
        else:
            return SignalDirection.HOLD
    
    def _check_timeframe_agreement(
        self,
        predictions: Dict[str, PredictionData]
    ) -> Tuple[bool, Optional[SignalDirection]]:
        """
        Check if both 1hour and 1day predictions agree on direction.
        
        Args:
            predictions: Dict[timeframe -> PredictionData]
        
        Returns:
            Tuple of (agreement: bool, direction: SignalDirection or None)
        """
        # Check if we have both timeframes
        if '1 hour' not in predictions or '1 day' not in predictions:
            return False, None
        
        pred_1hour = predictions['1 hour']
        pred_1day = predictions['1 day']
        
        direction_1hour = self._determine_direction(pred_1hour)
        direction_1day = self._determine_direction(pred_1day)
        
        # Both must agree and neither can be HOLD
        if direction_1hour == direction_1day and direction_1hour != SignalDirection.HOLD:
            return True, direction_1hour
        
        return False, None
    
    def _calculate_signal(
        self,
        symbol: str,
        direction: SignalDirection,
        predictions: Dict[str, PredictionData],
        market: MarketData,
        is_personal: bool
    ) -> SignalData:
        """
        Calculate signal parameters including entry, targets, and stop loss.
        
        Args:
            symbol: Trading pair symbol
            direction: Signal direction (LONG or SHORT)
            predictions: Dict of predictions by timeframe
            market: Current market data
            is_personal: Whether this is a personal prediction
        
        Returns:
            SignalData object
        """
        pred_1day = predictions['1 day']
        pred_1hour = predictions.get('1 hour')
        
        current_close = market.current_close
        atr = market.atr
        
        # Entry price is current market price
        entry_price = current_close
        
        # Target 1 is based on 1-day predicted close
        target_price_1 = pred_1day.predicted_close
        
        # Calculate additional targets and stop loss using ATR
        if direction == SignalDirection.LONG:
            target_price_2 = entry_price + (atr * self.config.atr_multiplier_tp2)
            target_price_3 = entry_price + (atr * self.config.atr_multiplier_tp3)
            stop_loss = entry_price - (atr * self.config.atr_multiplier_sl)
        else:  # SHORT
            target_price_2 = entry_price - (atr * self.config.atr_multiplier_tp2)
            target_price_3 = entry_price - (atr * self.config.atr_multiplier_tp3)
            stop_loss = entry_price + (atr * self.config.atr_multiplier_sl)
        
        # Calculate confidence score based on predicted move magnitude
        move_pct = abs(pred_1day.predicted_close - current_close) / current_close * 100
        confidence = min(
            self.config.max_confidence,
            self.config.base_confidence + (move_pct * self.config.move_pct_multiplier)
        )
        
        # Calculate expiry time
        time_expiry = datetime.now(timezone.utc) + timedelta(hours=self.config.default_expiry_hours)
        
        # Build signal data
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
    
    async def _determine_signal_status(
        self,
        direction: str,
        actual_close: float,
        entry_price: float,
        target_price: float,
        stop_loss: float
    ) -> str:
        """
        Determine signal resolution status based on actual close price.
        
        Args:
            direction: Signal direction ('LONG' or 'SHORT')
            actual_close: Actual closing price
            entry_price: Signal entry price
            target_price: Target price
            stop_loss: Stop loss price
        
        Returns:
            Signal status string
        """
        if direction == 'LONG':
            if actual_close >= target_price:
                return SignalStatus.HIT_TARGET.value
            elif actual_close <= stop_loss:
                return SignalStatus.HIT_SL.value
            else:
                return SignalStatus.CLOSED.value
        else:  # SHORT
            if actual_close <= target_price:
                return SignalStatus.HIT_TARGET.value
            elif actual_close >= stop_loss:
                return SignalStatus.HIT_SL.value
            else:
                return SignalStatus.CLOSED.value
    
    # ==================== DATABASE OPERATIONS ====================
    
    async def _save_signal(self, signal: SignalData) -> int:
        """
        Save signal to database.
        
        Args:
            signal: SignalData object
        
        Returns:
            Signal ID (primary key)
        """
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
        
        # Use 1day prediction ID as the main linked prediction
        daily_pred_id = signal.daily_prediction_id_1day
        personal_pred_id = signal.personal_prediction_id_1day
        
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
                daily_pred_id,
                personal_pred_id
            )
        
        return signal_id
    
    async def _check_existing_signal(
        self,
        symbol: str,
        predictions: Dict[str, PredictionData],
        is_personal: bool
    ) -> bool:
        """
        Check if a signal already exists for these predictions.
        
        Args:
            symbol: Trading pair symbol
            predictions: Dict of predictions by timeframe
            is_personal: Whether this is a personal prediction
        
        Returns:
            True if signal exists, False otherwise
        """
        pred_1day = predictions.get('1 day')
        if not pred_1day:
            return False
        
        async with self.pool.acquire() as conn:
            if is_personal:
                exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM signals
                        WHERE personal_prediction_id = $1
                          AND symbol = $2
                    )
                """, pred_1day.prediction_id, symbol)
            else:
                exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM signals
                        WHERE daily_prediction_id = $1
                          AND symbol = $2
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
        """
        Complete workflow to generate and save a signal for one symbol.
        
        Args:
            symbol: Trading pair symbol
            predictions: Dict[timeframe -> PredictionData]
            is_personal: Whether this is a personal prediction
        
        Returns:
            Dict with result
        """
        # Check if signal already exists
        if await self._check_existing_signal(symbol, predictions, is_personal):
            return {
                'success': False,
                'reason': 'Signal already exists for this prediction'
            }
        
        # Check if both timeframes agree
        agreement, direction = self._check_timeframe_agreement(predictions)
        if not agreement:
            timeframes = list(predictions.keys())
            if len(timeframes) < 2:
                reason = f'Missing timeframe data (have: {timeframes})'
            else:
                dir_1h = self._determine_direction(predictions.get('1 hour'))
                dir_1d = self._determine_direction(predictions.get('1 day'))
                reason = f'Timeframes disagree (1hour: {dir_1h.value}, 1day: {dir_1d.value})'
            
            logger.debug(f"{symbol}: {reason}")
            return {'success': False, 'reason': reason}
        
        # Get market data
        market = await self._get_market_data(symbol)
        if not market:
            return {'success': False, 'reason': 'Market data not available'}
        
        # Calculate signal
        signal = self._calculate_signal(symbol, direction, predictions, market, is_personal)
        
        # Save signal
        try:
            signal_id = await self._save_signal(signal)
            logger.info(
                f"Generated signal for {symbol}: {signal.direction.value} @ "
                f"{signal.entry_price:.2f} (confidence: {signal.confidence_score}%)"
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
    
    # ==================== UTILITY METHODS ====================
    
    async def get_active_signals(
        self,
        user_id: Optional[int] = None,
        symbol: Optional[str] = None,
        custom_indicator_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve active signals with optional filtering.
        
        Args:
            user_id: Filter by user (for personal signals)
            symbol: Filter by symbol
            custom_indicator_id: Filter by custom indicator
        
        Returns:
            List of signal dictionaries
        """
        query = """
            SELECT 
                s.id, s.symbol, s.direction, s.entry_price,
                s.target_price_1, s.target_price_2, s.target_price_3,
                s.stop_loss, s.confidence_score, s.time_generated,
                s.time_expiry, s.status, s.custom_indicator_id,
                s.daily_prediction_id, s.personal_prediction_id
            FROM signals s
            WHERE s.status = 'active'
        """
        params = []
        param_count = 1
        
        if symbol:
            query += f" AND s.symbol = ${param_count}"
            params.append(symbol)
            param_count += 1
        
        if custom_indicator_id:
            query += f" AND s.custom_indicator_id = ${param_count}"
            params.append(custom_indicator_id)
            param_count += 1
        
        if user_id:
            query += f""" 
                AND s.personal_prediction_id IN (
                    SELECT id FROM personal_daily_predictions WHERE user_id = ${param_count}
                )
            """
            params.append(user_id)
        
        query += " ORDER BY s.time_generated DESC"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        signals = []
        for row in rows:
            signals.append({
                'id': row['id'],
                'symbol': row['symbol'],
                'direction': row['direction'],
                'entry_price': float(row['entry_price']),
                'target_price_1': float(row['target_price_1']) if row['target_price_1'] else None,
                'target_price_2': float(row['target_price_2']) if row['target_price_2'] else None,
                'target_price_3': float(row['target_price_3']) if row['target_price_3'] else None,
                'stop_loss': float(row['stop_loss']) if row['stop_loss'] else None,
                'confidence_score': float(row['confidence_score']),
                'time_generated': row['time_generated'],
                'time_expiry': row['time_expiry'],
                'status': row['status'],
                'custom_indicator_id': row['custom_indicator_id'],
                'daily_prediction_id': row['daily_prediction_id'],
                'personal_prediction_id': row['personal_prediction_id']
            })
        
        return signals


# ==================== STANDALONE EXECUTION ====================

async def main():
    """
    Main execution function for running as a standalone script.
    Can be called from CLI or GitHub Actions.
    """
    print("=" * 80)
    print("SIGNAL GENERATOR")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    
    # Generate all global signals
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
    
    # Generate all personal signals
    print("\n" + "-" * 80)
    print("Generating PERSONAL signals for all users...")
    
    try:
        # Get all users with live indicators
        async with SignalGenerator() as generator:
            async with generator.pool.acquire() as conn:
                users = await conn.fetch("""
                    SELECT DISTINCT user_id
                    FROM user_custom_indicators
                    WHERE status = 'live'
                """)
        
        user_ids = [row['user_id'] for row in users]
        print(f"Found {len(user_ids)} users with live indicators")
        
        total_personal_generated = 0
        total_personal_skipped = 0
        
        # Process all users
        for user_id in user_ids:
            print(f"\nUser {user_id}:")
            async with SignalGenerator() as generator:
                stats = await generator.generate(prediction_type='personal', user_id=user_id)
            
            print(f"  Signals generated: {stats['signals_generated']}")
            print(f"  Signals skipped: {stats['signals_skipped']}")
            
            total_personal_generated += stats['signals_generated']
            total_personal_skipped += stats['signals_skipped']
        
        print(f"\nTotal Personal Signals:")
        print(f"  Generated: {total_personal_generated}")
        print(f"  Skipped: {total_personal_skipped}")
    
    except Exception as e:
        logger.error(f"Error processing personal signals: {e}")
        print(f"Error processing personal signals: {e}")
    
    print("\n" + "=" * 80)
    print("Signal generation complete")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

# # generate_signals.py
# # Signal generation system for crypto trading predictions
# # Generates signals only when BOTH 1-hour and 1-day predictions agree on direction

# import os
# import psycopg2
# from datetime import datetime, timedelta, timezone
# from typing import Optional, Dict, List, Tuple
# from enum import Enum
# from dataclasses import dataclass
# from dotenv import load_dotenv

# load_dotenv()


# class SignalDirection(Enum):
#     """Signal direction types"""
#     LONG = "LONG"
#     SHORT = "SHORT"
#     HOLD = "HOLD"


# class SignalStatus(Enum):
#     """Signal status types"""
#     ACTIVE = "active"
#     HIT_TARGET = "hit_target"
#     HIT_SL = "hit_sl"
#     EXPIRED = "expired"
#     CANCELLED = "cancelled"


# @dataclass
# class SignalConfig:
#     """Configuration for signal generation"""
#     # Risk management
#     atr_multiplier_sl: float = 1.5  # Stop loss ATR multiplier
#     atr_multiplier_tp1: float = 2.0  # Target 1 ATR multiplier
#     atr_multiplier_tp2: float = 3.0  # Target 2 ATR multiplier
#     atr_multiplier_tp3: float = 4.0  # Target 3 ATR multiplier
    
#     # Confidence calculation
#     base_confidence: float = 50.0  # Base confidence score
#     move_pct_multiplier: float = 10.0  # Move percentage to confidence conversion
#     max_confidence: float = 100.0
    
#     # Direction thresholds
#     direction_threshold_multiplier: float = 0.3  # Percentage of ATR for direction
    
#     # Signal expiry
#     default_expiry_hours: int = 24  # Default signal expiration in hours
    
#     # Timeframe agreement required
#     require_both_timeframes: bool = True  # Require both 1hour and 1day to agree


# @dataclass
# class PredictionData:
#     """Container for prediction data"""
#     prediction_id: int
#     symbol: str
#     timeframe: str
#     predicted_open: float
#     predicted_high: float
#     predicted_low: float
#     predicted_close: float
#     for_date: datetime
#     custom_indicator_id: Optional[int] = None
#     user_id: Optional[int] = None
#     is_personal: bool = False


# @dataclass
# class MarketData:
#     """Container for current market data"""
#     symbol: str
#     current_close: float
#     atr: float
#     timestamp: datetime


# @dataclass
# class SignalData:
#     """Container for generated signal data"""
#     symbol: str
#     direction: SignalDirection
#     entry_price: float
#     target_price_1: float
#     target_price_2: Optional[float] = None
#     target_price_3: Optional[float] = None
#     stop_loss: float = None
#     confidence_score: float = 50.0
#     time_expiry: Optional[datetime] = None
#     custom_indicator_id: Optional[int] = None
#     daily_prediction_id_1hour: Optional[int] = None
#     daily_prediction_id_1day: Optional[int] = None
#     personal_prediction_id_1hour: Optional[int] = None
#     personal_prediction_id_1day: Optional[int] = None


# class Signals:
#     """
#     Signal generation and management system for crypto trading.
    
#     This class generates trading signals based on predictions from both global 
#     and personalized models. Signals are only generated when BOTH 1-hour and 
#     1-day predictions agree on the direction (bullish or bearish).
    
#     Logic:
#     1. Fetch predictions for tomorrow's date
#     2. Group predictions by symbol
#     3. Check if BOTH 1hour and 1day timeframes exist for the symbol
#     4. Compare predicted_close vs predicted_open for each timeframe
#     5. If BOTH timeframes agree (both bullish or both bearish), generate signal
#     6. Calculate entry, targets, and stop loss using current market data
    
#     Usage:
#         # Generate global signals
#         with Signals() as signal_gen:
#             stats = signal_gen.generate(prediction_type='global')
        
#         # Generate personal signals for a user
#         with Signals() as signal_gen:
#             stats = signal_gen.generate(prediction_type='personal', user_id=5)
#     """
    
#     def __init__(self, database_url: str = None, config: SignalConfig = None):
#         """
#         Initialize the Signals generator.
        
#         Args:
#             database_url: PostgreSQL connection string (defaults to env variable)
#             config: SignalConfig instance for customization
#         """
#         self.database_url = database_url or os.getenv("DATABASE_URL")
#         if self.database_url and "sslmode" not in self.database_url:
#             self.database_url += "?sslmode=require"
        
#         self.config = config or SignalConfig()
#         self.conn = None
#         self.cur = None
    
#     def connect(self):
#         """Establish database connection"""
#         if not self.conn or self.conn.closed:
#             self.conn = psycopg2.connect(self.database_url)
#             self.cur = self.conn.cursor()
    
#     def disconnect(self):
#         """Close database connection"""
#         if self.cur:
#             self.cur.close()
#         if self.conn:
#             self.conn.close()
    
#     def __enter__(self):
#         """Context manager entry"""
#         self.connect()
#         return self
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         """Context manager exit"""
#         if self.conn and not self.conn.closed:
#             if exc_type is None:
#                 self.conn.commit()
#             else:
#                 self.conn.rollback()
#         self.disconnect()
    
#     # ==================== CORE SIGNAL GENERATION ====================
    
#     def generate(
#         self,
#         prediction_type: str = 'global',
#         user_id: int = None,
#         symbols: List[str] = None,
#         for_date: datetime = None,
#         auto_commit: bool = True
#     ) -> Dict[str, any]:
#         """
#         Main signal generation method.
        
#         Args:
#             prediction_type: 'global' or 'personal'
#             user_id: User ID for personal predictions (required if prediction_type='personal')
#             symbols: Filter by specific symbols (e.g., ['BTCUSD', 'ETHUSD'])
#             for_date: Target date for predictions (defaults to tomorrow)
#             auto_commit: Whether to commit changes automatically
        
#         Returns:
#             Dict with statistics: {
#                 'total_symbols': int,
#                 'signals_generated': int,
#                 'signals_skipped': int,
#                 'errors': int,
#                 'details': List[Dict]
#             }
#         """
#         stats = {
#             'total_symbols': 0,
#             'signals_generated': 0,
#             'signals_skipped': 0,
#             'errors': 0,
#             'details': []
#         }
        
#         try:
#             self.connect()
            
#             # Fetch predictions based on type
#             if prediction_type == 'global':
#                 predictions_by_symbol = self._fetch_global_predictions_grouped(symbols, for_date)
#             elif prediction_type == 'personal':
#                 if not user_id:
#                     raise ValueError("user_id is required for personal prediction type")
#                 predictions_by_symbol = self._fetch_personal_predictions_grouped(user_id, symbols, for_date)
#             else:
#                 raise ValueError(f"Invalid prediction_type: {prediction_type}. Use 'global' or 'personal'")
            
#             stats['total_symbols'] = len(predictions_by_symbol)
            
#             print("predictions_by_symbol.items() ", len(predictions_by_symbol.items()))
#             # Generate signals for each symbol
#             for symbol, predictions in predictions_by_symbol.items():
#                 try:
#                     result = self._generate_signal_for_symbol(symbol, predictions, prediction_type == 'personal')
                    
#                     if result['success']:
#                         stats['signals_generated'] += 1
#                         stats['details'].append({
#                             'symbol': symbol,
#                             'status': 'generated',
#                             'signal_id': result.get('signal_id'),
#                             'direction': result.get('direction'),
#                             'confidence': result.get('confidence')
#                         })
#                     else:
#                         stats['signals_skipped'] += 1
#                         stats['details'].append({
#                             'symbol': symbol,
#                             'status': 'skipped',
#                             'reason': result.get('reason')
#                         })
                
#                 except Exception as e:
#                     stats['errors'] += 1
#                     stats['details'].append({
#                         'symbol': symbol,
#                         'status': 'error',
#                         'error': str(e)
#                     })
#                     print(f"Error generating signal for {symbol}: {e}")
            
#             if auto_commit:
#                 self.conn.commit()
            
#         except Exception as e:
#             print(f"Critical error in generate(): {e}")
#             if self.conn:
#                 self.conn.rollback()
#             raise
#         finally:
#             if auto_commit:
#                 self.disconnect()
        
#         return stats
    
#     def resolve(self, signal_id: int, actual_close: float) -> Dict[str, any]:
#         """
#         Resolve a single signal based on actual close price.
        
#         Args:
#             signal_id: ID of the signal to resolve
#             actual_close: The real closing price on expiry date
        
#         Returns:
#             Dict with resolution result: {'success': bool, 'status': str, 'reason': str}
#         """
#         try:
#             # Fetch signal details
#             self.cur.execute("""
#                 SELECT 
#                     id, symbol, direction, entry_price,
#                     target_price_1, stop_loss, status,
#                     time_expiry
#                 FROM signals
#                 WHERE id = %s
#             """, (signal_id,))
            
#             row = self.cur.fetchone()
#             if not row:
#                 return {'success': False, 'reason': f'Signal ID {signal_id} not found'}
            
#             sig_id, symbol, direction, entry_price, tp1, sl, current_status, expiry = row
            
#             # Skip if already resolved
#             if current_status in ['hit_target', 'hit_sl', 'expired', 'cancelled']:
#                 return {'success': False, 'reason': f'Signal already resolved as {current_status}'}
            
#             # Determine final status
#             final_status = 'expired'  # default
            
#             if direction == 'LONG':
#                 if actual_close >= tp1:
#                     final_status = 'hit_target'
#                 elif actual_close <= sl:
#                     final_status = 'hit_sl'
#                 else:
#                     final_status = 'closed'   
#             elif direction == 'SHORT':
#                 if actual_close <= tp1:
#                     final_status = 'hit_target'
#                 elif actual_close >= sl:
#                     final_status = 'hit_sl'
#                 else:
#                     final_status = 'closed'    
            
#             # Update signal
#             self.cur.execute("""
#                 UPDATE signals
#                 SET 
#                     status = %s,
#                     updated_at = NOW()
#                 WHERE id = %s
#             """, (final_status, signal_id))
            
#             print(f"Signal {signal_id} ({symbol}) resolved: {final_status} | Actual close: {actual_close:.2f}")
            
#             return {
#                 'success': True,
#                 'status': final_status,
#                 'signal_id': signal_id,
#                 'symbol': symbol,
#                 'actual_close': actual_close
#             }
        
#         except Exception as e:
#             print(f"Error resolving signal {signal_id}: {e}")
#             return {'success': False, 'reason': str(e)}
#         # ==================== PREDICTION FETCHING ====================
    
#     def _fetch_global_predictions_grouped(
#         self,
#         symbols: List[str] = None,
#         for_date: datetime = None
#     ) -> Dict[str, Dict[str, PredictionData]]:
#         """
#         Fetch global predictions grouped by symbol and timeframe.
        
#         Returns:
#             Dict[symbol -> Dict[timeframe -> PredictionData]]
#         """
#         query = """
#             SELECT 
#                 id, symbol, timeframe,
#                 predicted_open, predicted_high, predicted_low, predicted_close,
#                 for_date
#             FROM daily_predictions
#             WHERE resolved_at IS NULL
#               AND timeframe IN ('1 hour', '1 day')
#         """
#         params = []
        
#         if symbols:
#             query += " AND symbol = ANY(%s)"
#             params.append(symbols)
        
#         if for_date:
#             query += " AND for_date = %s"
#             params.append(for_date)
#         else:
#             # Default to tomorrow's predictions
#             tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
#             query += " AND for_date = %s"
#             params.append(tomorrow)
        
#         query += " ORDER BY symbol, timeframe"

#         print("query ",query)
        
#         self.cur.execute(query, params)
#         rows = self.cur.fetchall()
        
#         # Group by symbol, then by timeframe
#         predictions_by_symbol = {}
#         for row in rows:
#             pred = PredictionData(
#                 prediction_id=row[0],
#                 symbol=row[1],
#                 timeframe=row[2],
#                 predicted_open=float(row[3]) if row[3] else 0.0,
#                 predicted_high=float(row[4]) if row[4] else 0.0,
#                 predicted_low=float(row[5]) if row[5] else 0.0,
#                 predicted_close=float(row[6]) if row[6] else 0.0,
#                 for_date=row[7],
#                 is_personal=False
#             )
            
#             if pred.symbol not in predictions_by_symbol:
#                 predictions_by_symbol[pred.symbol] = {}
            
#             predictions_by_symbol[pred.symbol][pred.timeframe] = pred
        
#         return predictions_by_symbol
    
#     def _fetch_personal_predictions_grouped(
#         self,
#         user_id: int,
#         symbols: List[str] = None,
#         for_date: datetime = None
#     ) -> Dict[str, Dict[str, PredictionData]]:
#         """
#         Fetch personal predictions grouped by symbol and timeframe.
        
#         Returns:
#             Dict[symbol -> Dict[timeframe -> PredictionData]]
#         """
#         query = """
#             SELECT 
#                 id, user_id, symbol, timeframe,
#                 predicted_open, predicted_high, predicted_low, predicted_close,
#                 for_date, custom_indicator_id
#             FROM personal_daily_predictions
#             WHERE resolved_at IS NULL
#               AND user_id = %s
#               AND timeframe IN ('1 hour', '1 day')
#         """
#         params = [user_id]
        
#         if symbols:
#             query += " AND symbol = ANY(%s)"
#             params.append(symbols)
        
#         if for_date:
#             query += " AND for_date = %s"
#             params.append(for_date)
#         else:
#             # Default to tomorrow's predictions
#             tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
#             query += " AND for_date = %s"
#             params.append(tomorrow)
        
#         query += " ORDER BY symbol, timeframe"
        
#         self.cur.execute(query, params)
#         rows = self.cur.fetchall()
        
#         # Group by symbol, then by timeframe
#         predictions_by_symbol = {}
#         for row in rows:
#             pred = PredictionData(
#                 prediction_id=row[0],
#                 user_id=row[1],
#                 symbol=row[2],
#                 timeframe=row[3],
#                 predicted_open=float(row[4]) if row[4] else 0.0,
#                 predicted_high=float(row[5]) if row[5] else 0.0,
#                 predicted_low=float(row[6]) if row[6] else 0.0,
#                 predicted_close=float(row[7]) if row[7] else 0.0,
#                 for_date=row[8],
#                 custom_indicator_id=row[9],
#                 is_personal=True
#             )
            
#             if pred.symbol not in predictions_by_symbol:
#                 predictions_by_symbol[pred.symbol] = {}
            
#             predictions_by_symbol[pred.symbol][pred.timeframe] = pred
        
#         return predictions_by_symbol
    
#     # ==================== MARKET DATA ====================
    
#     def _get_market_data(self, symbol: str) -> Optional[MarketData]:
#         """
#         Fetch latest market data for a symbol.
        
#         Args:
#             symbol: Trading pair symbol (e.g., 'BTCUSD')
        
#         Returns:
#             MarketData object or None if not available
#         """
#         self.cur.execute("""
#             SELECT close, atr, timestamp
#             FROM crypto_candles
#             WHERE symbol = %s 
#               AND timeframe = '1day'
#             ORDER BY timestamp DESC
#             LIMIT 1
#         """, (symbol,))
        
#         row = self.cur.fetchone()
#         if not row:
#             return None
        
#         close, atr, timestamp = row
        
#         # Validate data
#         if close is None:
#             print(f"  Warning: Latest close is NULL for {symbol}")
#             return None
        
#         if atr is None or atr <= 0:
#             print(f"  Warning: ATR is missing or invalid for {symbol}")
#             return None
        
#         return MarketData(
#             symbol=symbol,
#             current_close=float(close),
#             atr=float(atr),
#             timestamp=timestamp
#         )
    
#     # ==================== SIGNAL CALCULATION ====================
    
#     def _determine_direction(self, pred: PredictionData) -> SignalDirection:
#         """
#         Determine direction from a single prediction.
        
#         Args:
#             pred: PredictionData object
        
#         Returns:
#             SignalDirection (LONG, SHORT, or HOLD)
#         """
#         print("pred Signal ", pred)

#         if pred.predicted_close > pred.predicted_open:
#             return SignalDirection.LONG
#         elif pred.predicted_close < pred.predicted_open:
#             return SignalDirection.SHORT
#         else:
#             return SignalDirection.HOLD
    
#     def _check_timeframe_agreement(
#         self,
#         predictions: Dict[str, PredictionData]
#     ) -> Tuple[bool, Optional[SignalDirection]]:
#         """
#         Check if both 1hour and 1day predictions agree on direction.
        
#         Args:
#             predictions: Dict[timeframe -> PredictionData]
        
#         Returns:
#             Tuple of (agreement: bool, direction: SignalDirection or None)
#         """
#         # Check if we have both timeframes
#         if '1 hour' not in predictions or '1 day' not in predictions:
#             return False, None
        
#         pred_1hour = predictions['1 hour']
#         pred_1day = predictions['1 day']
        
#         direction_1hour = self._determine_direction(pred_1hour)
#         direction_1day = self._determine_direction(pred_1day)
        
#         # Both must agree and neither can be HOLD
#         if direction_1hour == direction_1day and direction_1hour != SignalDirection.HOLD:
#             return True, direction_1hour
        
#         return False, None
    
#     def _calculate_signal(
#         self,
#         symbol: str,
#         direction: SignalDirection,
#         predictions: Dict[str, PredictionData],
#         market: MarketData,
#         is_personal: bool
#     ) -> SignalData:
#         """
#         Calculate signal parameters.
        
#         Args:
#             symbol: Trading pair symbol
#             direction: Signal direction (LONG or SHORT)
#             predictions: Dict of predictions by timeframe
#             market: Current market data
#             is_personal: Whether this is a personal prediction
        
#         Returns:
#             SignalData object
#         """
#         pred_1day = predictions['1 day']
#         pred_1hour = predictions.get('1 hour')
        
#         current_close = market.current_close
#         atr = market.atr
        
#         # Entry price is current market price
#         entry_price = current_close
        
#         # Target 1 is based on 1-day predicted close
#         target_price_1 = pred_1day.predicted_close
        
#         # Calculate additional targets and stop loss using ATR
#         if direction == SignalDirection.LONG:
#             target_price_2 = entry_price + (atr * self.config.atr_multiplier_tp2)
#             target_price_3 = entry_price + (atr * self.config.atr_multiplier_tp3)
#             stop_loss = entry_price - (atr * self.config.atr_multiplier_sl)
#         else:  # SHORT
#             target_price_2 = entry_price - (atr * self.config.atr_multiplier_tp2)
#             target_price_3 = entry_price - (atr * self.config.atr_multiplier_tp3)
#             stop_loss = entry_price + (atr * self.config.atr_multiplier_sl)
        
#         # Calculate confidence score based on predicted move magnitude
#         move_pct = abs(pred_1day.predicted_close - current_close) / current_close * 100
#         confidence = min(
#             self.config.max_confidence,
#             self.config.base_confidence + (move_pct * self.config.move_pct_multiplier)
#         )
        
#         # Calculate expiry time
#         time_expiry = datetime.now(timezone.utc) + timedelta(hours=self.config.default_expiry_hours)
        
#         # Build signal data
#         signal = SignalData(
#             symbol=symbol,
#             direction=direction,
#             entry_price=entry_price,
#             target_price_1=target_price_1,
#             target_price_2=target_price_2,
#             target_price_3=target_price_3,
#             stop_loss=stop_loss,
#             confidence_score=round(confidence, 2),
#             time_expiry=time_expiry,
#             custom_indicator_id=pred_1day.custom_indicator_id if is_personal else None
#         )
        
#         # Link prediction IDs
#         if is_personal:
#             signal.personal_prediction_id_1day = pred_1day.prediction_id
#             signal.personal_prediction_id_1hour = pred_1hour.prediction_id if pred_1hour else None
#         else:
#             signal.daily_prediction_id_1day = pred_1day.prediction_id
#             signal.daily_prediction_id_1hour = pred_1hour.prediction_id if pred_1hour else None
        
#         return signal
    
#     # ==================== DATABASE OPERATIONS ====================
    
#     def _save_signal(self, signal: SignalData) -> int:
#         """
#         Save signal to database.
        
#         Args:
#             signal: SignalData object
        
#         Returns:
#             Signal ID (primary key)
#         """
#         insert_sql = """
#             INSERT INTO signals (
#                 custom_indicator_id,
#                 symbol, direction, entry_price,
#                 target_price_1, target_price_2, target_price_3,
#                 stop_loss, confidence_score,
#                 time_generated, time_expiry,
#                 status,
#                 daily_prediction_id, personal_prediction_id
#             ) VALUES (
#                 %s, %s, %s, %s,
#                 %s, %s, %s,
#                 %s, %s,
#                 NOW(), %s,
#                 %s,
#                 %s, %s
#             )
#             RETURNING id
#         """
        
#         # Use 1day prediction ID as the main linked prediction
#         daily_pred_id = signal.daily_prediction_id_1day
#         personal_pred_id = signal.personal_prediction_id_1day
        
#         self.cur.execute(insert_sql, (
#             signal.custom_indicator_id,
#             signal.symbol,
#             signal.direction.value,
#             signal.entry_price,
#             signal.target_price_1,
#             signal.target_price_2,
#             signal.target_price_3,
#             signal.stop_loss,
#             signal.confidence_score,
#             signal.time_expiry,
#             SignalStatus.ACTIVE.value,
#             daily_pred_id,
#             personal_pred_id
#         ))
        
#         signal_id = self.cur.fetchone()[0]
#         return signal_id
    
#     def _check_existing_signal(self, symbol: str, predictions: Dict[str, PredictionData], is_personal: bool) -> bool:
#         """
#         Check if a signal already exists for these predictions.
        
#         Args:
#             symbol: Trading pair symbol
#             predictions: Dict of predictions by timeframe
#             is_personal: Whether this is a personal prediction
        
#         Returns:
#             True if signal exists, False otherwise
#         """
#         pred_1day = predictions.get('1 day') or predictions.get('1day')
#         if not pred_1day:
#             return False
        
#         if is_personal:
#             self.cur.execute("""
#                 SELECT id FROM signals
#                 WHERE personal_prediction_id = %s
#                   AND symbol = %s
#                 LIMIT 1
#             """, (pred_1day.prediction_id, symbol))
#         else:
#             self.cur.execute("""
#                 SELECT id FROM signals
#                 WHERE daily_prediction_id = %s
#                   AND symbol = %s
#                 LIMIT 1
#             """, (pred_1day.prediction_id, symbol))
        
#         return self.cur.fetchone() is not None
    
#     # ==================== WORKFLOW METHODS ====================
    
#     def _generate_signal_for_symbol(
#         self,
#         symbol: str,
#         predictions: Dict[str, PredictionData],
#         is_personal: bool
#     ) -> Dict:
#         """
#         Complete workflow to generate and save a signal for one symbol.
        
#         Args:
#             symbol: Trading pair symbol
#             predictions: Dict[timeframe -> PredictionData]
#             is_personal: Whether this is a personal prediction
        
#         Returns:
#             Dict with result: {'success': bool, 'reason': str, 'signal_id': int, ...}
#         """
        
#         print("_generate_signal_for_symbol ")

#         # Check if signal already exists
#         if self._check_existing_signal(symbol, predictions, is_personal):
#             return {
#                 'success': False,
#                 'reason': 'Signal already exists for this symbol'
#             }
        
#         # Check if both timeframes agree
#         agreement, direction = self._check_timeframe_agreement(predictions)
#         if not agreement:
#             timeframes = list(predictions.keys())
#             print("timeframes ", timeframes)
#             if len(timeframes) < 2:
#                 reason = f'Missing timeframe data (have: {timeframes})'
#             else:
#                 dir_1h = self._determine_direction(predictions.get('1hour'))
#                 dir_1d = self._determine_direction(predictions.get('1day'))
#                 reason = f'Timeframes disagree (1hour: {dir_1h.value}, 1day: {dir_1d.value})'
            
#             return {
#                 'success': False,
#                 'reason': reason
#             }
        
#         # Get market data
#         market = self._get_market_data(symbol)
#         if not market:
#             return {
#                 'success': False,
#                 'reason': 'Market data not available'
#             }
        
#         # Calculate signal
#         signal = self._calculate_signal(symbol, direction, predictions, market, is_personal)
        
#         # Save signal
#         try:
#             signal_id = self._save_signal(signal)
#             return {
#                 'success': True,
#                 'signal_id': signal_id,
#                 'direction': signal.direction.value,
#                 'confidence': signal.confidence_score,
#                 'entry_price': signal.entry_price
#             }
#         except Exception as e:
#             return {
#                 'success': False,
#                 'reason': f'Database error: {str(e)}'
#             }
    
#     # ==================== UTILITY METHODS ====================
    
#     def get_active_signals(
#         self,
#         user_id: int = None,
#         symbol: str = None,
#         custom_indicator_id: int = None
#     ) -> List[Dict]:
#         """
#         Retrieve active signals with optional filtering.
        
#         Args:
#             user_id: Filter by user (for personal signals)
#             symbol: Filter by symbol
#             custom_indicator_id: Filter by custom indicator
        
#         Returns:
#             List of signal dictionaries
#         """
#         query = """
#             SELECT 
#                 s.id, s.symbol, s.direction, s.entry_price,
#                 s.target_price_1, s.target_price_2, s.target_price_3,
#                 s.stop_loss, s.confidence_score, s.time_generated,
#                 s.time_expiry, s.status, s.custom_indicator_id,
#                 s.daily_prediction_id, s.personal_prediction_id
#             FROM signals s
#             WHERE s.status = 'active'
#         """
#         params = []
        
#         if symbol:
#             query += " AND s.symbol = %s"
#             params.append(symbol)
        
#         if custom_indicator_id:
#             query += " AND s.custom_indicator_id = %s"
#             params.append(custom_indicator_id)
        
#         if user_id:
#             query += """ 
#                 AND s.personal_prediction_id IN (
#                     SELECT id FROM personal_daily_predictions WHERE user_id = %s
#                 )
#             """
#             params.append(user_id)
        
#         query += " ORDER BY s.time_generated DESC"
        
#         self.connect()
#         self.cur.execute(query, params)
#         rows = self.cur.fetchall()
#         self.disconnect()
        
#         signals = []
#         for row in rows:
#             signals.append({
#                 'id': row[0],
#                 'symbol': row[1],
#                 'direction': row[2],
#                 'entry_price': float(row[3]),
#                 'target_price_1': float(row[4]) if row[4] else None,
#                 'target_price_2': float(row[5]) if row[5] else None,
#                 'target_price_3': float(row[6]) if row[6] else None,
#                 'stop_loss': float(row[7]) if row[7] else None,
#                 'confidence_score': float(row[8]),
#                 'time_generated': row[9],
#                 'time_expiry': row[10],
#                 'status': row[11],
#                 'custom_indicator_id': row[12],
#                 'daily_prediction_id': row[13],
#                 'personal_prediction_id': row[14]
#             })
        
#         return signals


# # ==================== STANDALONE EXECUTION ====================

# def main():
#     """
#     Main execution function for running as a standalone script.
#     Can be called from CLI or GitHub Actions.
#     """
#     print("=" * 80)
#     print("SIGNAL GENERATOR")
#     print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
#     print("=" * 80)
    
#     # Generate all global signals
#     print("\nGenerating GLOBAL signals...")
#     with Signals() as signal_gen:
#         stats = signal_gen.generate(prediction_type='global', user_id=None, symbols=['BTCUSD','ETHUSD'])
    
#     print(f"\nGlobal Results:")
#     print(f"  Total symbols: {stats['total_symbols']}")
#     print(f"  Signals generated: {stats['signals_generated']}")
#     print(f"  Signals skipped: {stats['signals_skipped']}")
#     print(f"  Errors: {stats['errors']}")
    
#     if stats['details']:
#         print("\nDetails:")
#         for detail in stats['details']:
#             if detail['status'] == 'generated':
#                 print(f"  ✓ {detail['symbol']}: {detail['direction']} (Confidence: {detail['confidence']}%)")
#             elif detail['status'] == 'skipped':
#                 print(f"  - {detail['symbol']}: Skipped ({detail['reason']})")
#             elif detail['status'] == 'error':
#                 print(f"  ✗ {detail['symbol']}: Error ({detail['error']})")
    
#     # Generate all personal signals
#     print("\n" + "-" * 80)
#     print("Generating PERSONAL signals for all users...")
    
#     # Get all users with live indicators
#     try:
#         conn = psycopg2.connect(os.getenv("DATABASE_URL") + "?sslmode=require")
#         cur = conn.cursor()
#         cur.execute("""
#             SELECT DISTINCT user_id
#             FROM user_custom_indicators
#             WHERE status = 'live'
#         """)
#         users = [row[0] for row in cur.fetchall()]
#         cur.close()
#         conn.close()
        
#         print(f"Found {len(users)} users with live indicators")
        
#         total_personal_generated = 0
#         total_personal_skipped = 0
        
#         for user_id in users:
#             print(f"\nUser {user_id}:")
#             with Signals() as signal_gen:
#                 stats = signal_gen.generate(prediction_type='personal', user_id=user_id)
            
#             print(f"  Signals generated: {stats['signals_generated']}")
#             print(f"  Signals skipped: {stats['signals_skipped']}")
            
#             total_personal_generated += stats['signals_generated']
#             total_personal_skipped += stats['signals_skipped']
        
#         print(f"\nTotal Personal Signals:")
#         print(f"  Generated: {total_personal_generated}")
#         print(f"  Skipped: {total_personal_skipped}")
    
#     except Exception as e:
#         print(f"Error processing personal signals: {e}")
    
#     print("\n" + "=" * 80)
#     print("Signal generation complete")
#     print("=" * 80)


# if __name__ == "__main__":
#     main()