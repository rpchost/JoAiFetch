# generate_signals.py - HOURLY PREDICTION-BASED SIGNALS
# New Logic:
# 1. Hourly predictions drive signal generation
# 2. Daily predictions provide confirmation (must align)
# 3. Signals auto-close when next prediction disagrees
# 4. Only one active signal per symbol+direction at a time

"""
Signal Generation System for Crypto Trading Predictions (HOURLY-BASED)

Signal Generation Logic:
1. Check if hourly and daily predictions ALIGN (candle direction):
   - LONG alignment: predicted_close > predicted_open for BOTH timeframes
   - SHORT alignment: predicted_close < predicted_open for BOTH timeframes
2. Verify second criteria (current price vs hourly target):
   - LONG: current_price < hourly predicted_close
   - SHORT: current_price > hourly predicted_close
3. Generate signal with target = hourly predicted_close
4. Auto-close signals when predictions change direction
5. Only one active signal per symbol+direction allowed
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

from signal_outcomes import LOGIC_VERSION, realized_r

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
    # Auto-closed signals use CLOSED plus exit_reason=prediction_changed;
    # status_enum has no prediction_changed member.


# ==================== DATA CLASSES ====================

@dataclass
class SignalConfig:
    """Configuration for signal generation and risk management"""
    
    # Risk management - ATR multipliers, both on the 1-hour series the signal
    # targets. Target and stop are symmetrical by construction, so reward:risk
    # is always atr_multiplier_tp / atr_multiplier_sl and the model contributes
    # direction only (its predicted close sits far too close to entry to serve
    # as a profit target).
    atr_multiplier_sl: float = 1.5
    atr_multiplier_tp: float = 2.0

    # Refuse to build a signal from a candle older than this. fetch_data.py runs
    # immediately before signal generation, so a stale candle means the feed is
    # broken; emitting an entry the market already left would be dishonest.
    market_data_max_age_hours: float = 3.0

    # Kept for compatibility only. Signals no longer expire by the clock, so
    # this value is not written to time_expiry any more.
    signal_expiry_hours: float = 4.0
    # Confidence calculation
    base_confidence: float = 50.0
    move_pct_multiplier: float = 10.0
    max_confidence: float = 100.0
    
    # Direction threshold (0.5% to avoid noise)
    direction_threshold_pct: float = 0.005  # 0.5%
    
    # Signal expiry (not used in hourly mode, but kept for compatibility)
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
    stop_loss: Optional[float] = None
    confidence_score: float = 50.0
    time_expiry: Optional[datetime] = None
    entry_candle_ts: Optional[datetime] = None
    atr_at_entry: Optional[float] = None
    custom_indicator_id: Optional[int] = None
    daily_prediction_id: Optional[int] = None
    hourly_prediction_id: Optional[int] = None
    personal_prediction_id_1hour: Optional[int] = None
    personal_prediction_id_1day: Optional[int] = None


# ==================== MAIN CLASS ====================

class SignalGenerator:
    """
    Async signal generation and management system for crypto trading.
    
    NEW LOGIC:
    - Hourly predictions drive signal generation
    - Daily predictions provide confirmation
    - Signals auto-close when next prediction disagrees
    - Only one active signal per symbol+direction
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
            'signals_closed': 0,
            'signals_skipped': 0,
            'errors': 0,
            'details': []
        }
        
        try:
            if prediction_type not in ['global', 'personal']:
                raise ValueError(f"Invalid prediction_type: {prediction_type}")
            
            if prediction_type == 'personal' and not user_id:
                raise ValueError("user_id required for personal predictions")
            
            # STEP 1: Auto-close signals with misaligned predictions
            close_stats = await self._auto_close_misaligned_signals(prediction_type, user_id, symbols, for_date)
            stats['signals_closed'] = close_stats['closed']
            
            # STEP 2: Fetch predictions
            if prediction_type == 'global':
                predictions_by_symbol = await self._fetch_global_predictions_grouped(symbols, for_date)
            else:
                predictions_by_symbol = await self._fetch_personal_predictions_grouped(user_id, symbols, for_date)
            
            stats['total_symbols'] = len(predictions_by_symbol)
            logger.info(f"Processing {stats['total_symbols']} symbols for {prediction_type} signals")
            
            # STEP 3: Generate signals concurrently
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
                f"{stats['signals_closed']} auto-closed, "
                f"{stats['signals_skipped']} skipped, {stats['errors']} errors"
            )
            
        except Exception as e:
            logger.error(f"Critical error in generate(): {e}")
            raise
        
        return stats
    
    # ==================== AUTO-CLOSE MISALIGNED SIGNALS ====================
    
    async def _auto_close_misaligned_signals(
        self,
        prediction_type: str,
        user_id: Optional[int],
        symbols: Optional[List[str]],
        for_date: Optional[datetime]
    ) -> Dict[str, int]:
        """
        Auto-close active signals where predictions no longer align with signal direction
        
        Returns: Dict with count of closed signals
        """
        stats = {'closed': 0}
        
        try:
            # Fetch active signals
            async with self.pool.acquire() as conn:
                if prediction_type == 'global':
                    query = """
                        SELECT 
                            s.id, s.symbol, s.direction,
                            s.entry_price, s.stop_loss,
                            s.daily_prediction_id, s.hourly_prediction_id
                        FROM signals s
                        WHERE s.status = 'active'
                          AND s.personal_prediction_id IS NULL
                          AND s.logic_version >= 2
                    """
                    params = []
                    
                    if symbols:
                        query += " AND s.symbol = ANY($1)"
                        params.append(symbols)
                    
                    signals = await conn.fetch(query, *params)
                else:
                    query = """
                        SELECT 
                            s.id, s.symbol, s.direction,
                            s.entry_price, s.stop_loss,
                            s.personal_prediction_id_1day, s.personal_prediction_id_1hour
                        FROM signals s
                        INNER JOIN personal_daily_predictions p ON s.personal_prediction_id_1day = p.id
                        WHERE s.status = 'active'
                          AND p.user_id = $1
                          AND s.logic_version >= 2
                    """
                    params = [user_id]
                    
                    if symbols:
                        query += " AND s.symbol = ANY($2)"
                        params.append(symbols)
                    
                    signals = await conn.fetch(query, *params)
            
            # Check each signal against current predictions
            for signal in signals:
                symbol = signal['symbol']
                signal_direction = signal['direction']
                signal_id = signal['id']
                
                # Fetch current predictions for this symbol
                if prediction_type == 'global':
                    predictions = await self._fetch_global_predictions_grouped([symbol], for_date)
                else:
                    predictions = await self._fetch_personal_predictions_grouped(user_id, [symbol], for_date)
                
                if symbol not in predictions:
                    continue
                
                preds = predictions[symbol]
                
                # Flip rule needs both views: the 1-hour and the 4-hour prediction.
                if '1 hour' not in preds or '4 hours' not in preds:
                    continue
                
                # Get market data
                market = await self._get_market_data(symbol)
                if not market:
                    continue
                
                # Check if predictions still align with signal
                hourly_direction = self._determine_direction(preds['1 hour'])
                four_hour_direction = self._determine_direction(preds['4 hours'])

                # Close only when BOTH the 1-hour and the 4-hour prediction point
                # against the position. One timeframe disagreeing is not enough:
                # it flips back too easily and would close trades that were fine.
                should_close = (
                    hourly_direction.value != signal_direction
                    and four_hour_direction.value != signal_direction
                )

                # Close signal if both timeframes are against it
                if should_close:
                    reason = (
                        f"1h {hourly_direction.value} and 4h "
                        f"{four_hour_direction.value} both against {signal_direction}"
                    )
                    exit_price = market.current_close
                    r_multiple = None
                    if signal['entry_price'] is not None and signal['stop_loss'] is not None:
                        r_multiple = realized_r(
                            signal['direction'],
                            float(signal['entry_price']),
                            float(signal['stop_loss']),
                            exit_price,
                        )
                    async with self.pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE signals
                            SET
                                status = $1,
                                exit_price = $2,
                                exit_reason = $3,
                                resolved_at = NOW(),
                                realized_r = $4,
                                updated_at = NOW()
                            WHERE id = $5
                        """,
                        SignalStatus.CLOSED.value, exit_price,
                        'prediction_changed', r_multiple, signal_id)
                    
                    stats['closed'] += 1
                    logger.info(f"⚠️ Auto-closed signal #{signal_id} ({symbol} {signal_direction}): {reason}")
        
        except Exception as e:
            logger.error(f"Error in auto-close: {e}")
        
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
              AND timeframe IN ('1 hour', '4 hours', '1 day')
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
              AND timeframe IN ('1 hour', '4 hours', '1 day')
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
        """
        Latest 1-hour candle: the price the market is trading at right now.

        Entry and stop-loss ATR must both come from the 1-hour series the signal
        targets, never from the daily candle (up to 24h stale, and its ATR is ~5x
        too wide for an hourly target). Returns None rather than pricing a signal
        off missing or stale data.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT close, atr, timestamp
                FROM crypto_candles
                WHERE symbol = $1
                  AND timeframe = '1hour'
                  AND close > 0 AND atr > 0
                ORDER BY timestamp DESC, id DESC
                LIMIT 1
                """,
                symbol,
            )

        if not row:
            logger.warning(f"{symbol}: no usable 1-hour candle")
            return None

        candle_timestamp = row['timestamp']
        age_hours = (
            datetime.now(timezone.utc) - candle_timestamp
        ).total_seconds() / 3600
        if age_hours > self.config.market_data_max_age_hours:
            logger.warning(
                f"{symbol}: stale 1-hour candle "
                f"({candle_timestamp.strftime('%Y-%m-%d %H:%M')} UTC, "
                f"{age_hours:.1f}h old, limit "
                f"{self.config.market_data_max_age_hours}h) - skipping signal"
            )
            return None

        logger.info(
            f"{symbol}: 1-hour candle "
            f"{candle_timestamp.strftime('%Y-%m-%d %H:%M')} UTC, "
            f"close ${float(row['close']):.2f}, atr ${float(row['atr']):.2f}"
        )

        return MarketData(
            symbol=symbol,
            current_close=float(row['close']),
            atr=float(row['atr']),
            timestamp=candle_timestamp
        )

    # ==================== SIGNAL CALCULATION ====================
    
    def _determine_direction(
        self, 
        pred: PredictionData
    ) -> SignalDirection:
        """
        Determine direction by comparing predicted_close to predicted_open
        
        CORRECT LOGIC:
        - LONG: predicted_close > predicted_open (bullish candle)
        - SHORT: predicted_close < predicted_open (bearish candle)
        - HOLD: predicted_close == predicted_open (exactly equal)
        
        NO THRESHOLD for alignment - any movement counts
        """
        if pred.predicted_close > pred.predicted_open:
            return SignalDirection.LONG
        elif pred.predicted_close < pred.predicted_open:
            return SignalDirection.SHORT
        else:
            return SignalDirection.HOLD
    
    def _check_timeframe_alignment(
        self,
        predictions: Dict[str, PredictionData],
        current_price: float
    ) -> Tuple[bool, Optional[SignalDirection], str]:
        """
        CORRECT LOGIC: Check if hourly and daily predictions ALIGN
        
        Alignment = both candles have same direction (close vs open)
        - LONG: predicted_close > predicted_open for BOTH
        - SHORT: predicted_close < predicted_open for BOTH
        
        Then check second criteria:
        - For LONG: current_price < hourly predicted_close
        - For SHORT: current_price > hourly predicted_close
        
        Returns: (aligned, direction, debug_info)
        """
        if '1 hour' not in predictions or '1 day' not in predictions:
            timeframes = list(predictions.keys())
            return False, None, f'Missing timeframes (have: {timeframes})'
        
        pred_1hour = predictions['1 hour']
        pred_1day = predictions['1 day']
        
        # STEP 1: Check alignment (candle direction: close vs open)
        direction_1hour = self._determine_direction(pred_1hour)
        direction_1day = self._determine_direction(pred_1day)
        
        debug_info = (
            f"1h: open=${pred_1hour.predicted_open:.2f} close=${pred_1hour.predicted_close:.2f} → {direction_1hour.value}, "
            f"1d: open=${pred_1day.predicted_open:.2f} close=${pred_1day.predicted_close:.2f} → {direction_1day.value}, "
            f"current=${current_price:.2f}"
        )
        
        # Both must align (both LONG or both SHORT) and neither can be HOLD
        if direction_1hour != direction_1day or direction_1hour == SignalDirection.HOLD:
            return False, None, f"No alignment: {debug_info}"
        
        # STEP 2: Check second criteria (current price vs hourly predicted close)
        if direction_1hour == SignalDirection.LONG:
            # For LONG: current price must be below hourly predicted close
            if current_price >= pred_1hour.predicted_close:
                return False, None, f"LONG aligned but current ${current_price:.2f} >= hourly target ${pred_1hour.predicted_close:.2f}"
        elif direction_1hour == SignalDirection.SHORT:
            # For SHORT: current price must be above hourly predicted close
            if current_price <= pred_1hour.predicted_close:
                return False, None, f"SHORT aligned but current ${current_price:.2f} <= hourly target ${pred_1hour.predicted_close:.2f}"
        
        return True, direction_1hour, debug_info
    
    def _calculate_signal(
        self,
        symbol: str,
        direction: SignalDirection,
        predictions: Dict[str, PredictionData],
        market: MarketData,
        is_personal: bool
    ) -> Optional[SignalData]:
        """
        Calculate signal parameters
        
        NEW LOGIC:
        - Entry = latest 1-hour close (the price the market is trading at)
        - Target = entry +/- ATR_1h * atr_multiplier_tp
        - Stop loss = entry -/+ ATR_1h * atr_multiplier_sl
        - No time horizon: a signal ends only at target, at stop, or on a flip
        """
        pred_1day = predictions['1 day']
        pred_1hour = predictions['1 hour']
        
        current_close = market.current_close
        atr = market.atr
        
        entry_price = current_close
        
        # Target and stop both derive from the same 1-hour ATR, so reward:risk is
        # fixed by configuration instead of depending on how far the model
        # happened to predict.
        if direction == SignalDirection.LONG:
            stop_loss = entry_price - (atr * self.config.atr_multiplier_sl)
            target_price_1 = entry_price + (atr * self.config.atr_multiplier_tp)
        else:  # SHORT
            stop_loss = entry_price + (atr * self.config.atr_multiplier_sl)
            target_price_1 = entry_price - (atr * self.config.atr_multiplier_tp)
        
        # Invariant: if the geometry stops being symmetrical the configuration
        # is inconsistent and nothing may be published.
        risk_distance = abs(entry_price - stop_loss)
        reward_distance = abs(target_price_1 - entry_price)
        expected_rr = self.config.atr_multiplier_tp / self.config.atr_multiplier_sl
        actual_rr = reward_distance / risk_distance if risk_distance > 0 else 0.0
        if abs(actual_rr - expected_rr) > 0.01:
            logger.error(
                f"{symbol}: reward:risk {actual_rr:.3f} does not match the "
                f"configured {expected_rr:.3f} (entry {entry_price:.2f}, target "
                f"{target_price_1:.2f}, stop {stop_loss:.2f}) - not publishing"
            )
            return None
        
        # Confidence score based on hourly prediction
        move_pct = abs(pred_1hour.predicted_close - current_close) / current_close * 100
        confidence = min(
            self.config.max_confidence,
            self.config.base_confidence + (move_pct * self.config.move_pct_multiplier)
        )
        
        # No deadline. A signal stays open until price touches its target or
        # stop, or until the predictions behind it flip. time_expiry is written
        # as NULL so nothing downstream can expire it by the clock.
        time_expiry = None
        
        signal = SignalData(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            target_price_1=target_price_1,
            stop_loss=stop_loss,
            confidence_score=round(confidence, 2),
            time_expiry=time_expiry,
            entry_candle_ts=market.timestamp,
            atr_at_entry=atr,
            custom_indicator_id=pred_1day.custom_indicator_id if is_personal else None
        )
        
        # Link prediction IDs
        if is_personal:
            signal.personal_prediction_id_1day = pred_1day.prediction_id
            signal.personal_prediction_id_1hour = pred_1hour.prediction_id
        else:
            signal.daily_prediction_id = pred_1day.prediction_id
            signal.hourly_prediction_id = pred_1hour.prediction_id
        
        return signal
    
    # ==================== DATABASE OPERATIONS ====================
    
    async def _save_signal(self, signal: SignalData) -> int:
        """Save signal to database"""
        insert_sql = """
            INSERT INTO signals (
                custom_indicator_id,
                symbol, direction, entry_price,
                target_price_1,
                stop_loss, confidence_score,
                time_generated, time_expiry,
                status,
                daily_prediction_id, hourly_prediction_id,
                personal_prediction_id, personal_prediction_id_1hour,
                entry_candle_ts, atr_at_entry, logic_version
            ) VALUES (
                $1, $2, $3, $4,
                $5,
                $6, $7,
                NOW(), $8,
                $9,
                $10, $11,
                $12, $13,
                $14, $15, $16
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
                signal.stop_loss,
                signal.confidence_score,
                signal.time_expiry,
                SignalStatus.ACTIVE.value,
                signal.daily_prediction_id,
                signal.hourly_prediction_id,
                signal.personal_prediction_id_1day,
                signal.personal_prediction_id_1hour,
                signal.entry_candle_ts,
                signal.atr_at_entry,
                LOGIC_VERSION
            )
        
        return signal_id
    
    async def _check_existing_active_signal(
        self,
        symbol: str,
        direction: SignalDirection,
        is_personal: bool,
        user_id: Optional[int] = None
    ) -> bool:
        """
        Check if an active signal already exists for this symbol+direction
        
        NEW RULE: Only one active signal per symbol+direction
        """
        async with self.pool.acquire() as conn:
            if is_personal and user_id:
                exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM signals s
                        INNER JOIN personal_daily_predictions p 
                            ON s.personal_prediction_id_1day = p.id
                        WHERE s.symbol = $1
                          AND s.direction = $2
                          AND s.status = 'active'
                          AND p.user_id = $3
                    )
                """, symbol, direction.value, user_id)
            else:
                exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM signals
                        WHERE symbol = $1
                          AND direction = $2
                          AND status = 'active'
                          AND personal_prediction_id IS NULL
                    )
                """, symbol, direction.value)
        
        return exists
    
    # ==================== WORKFLOW METHODS ====================
    
    async def _generate_signal_for_symbol(
        self,
        symbol: str,
        predictions: Dict[str, PredictionData],
        is_personal: bool
    ) -> Dict[str, Any]:
        """Complete workflow to generate signal for one symbol"""
        
        # Get market data
        market = await self._get_market_data(symbol)
        if not market:
            return {'success': False, 'reason': 'Market data not available'}
        
        # Check timeframe alignment
        aligned, direction, debug_info = self._check_timeframe_alignment(
            predictions, 
            market.current_close
        )
        
        if not aligned:
            logger.debug(f"{symbol}: Predictions not aligned - {debug_info}")
            return {'success': False, 'reason': f'Predictions not aligned ({debug_info})'}
        
        # Check if active signal already exists for this symbol+direction
        user_id = predictions['1 day'].user_id if is_personal else None
        if await self._check_existing_active_signal(symbol, direction, is_personal, user_id):
            return {
                'success': False,
                'reason': f'Active {direction.value} signal already exists for {symbol}'
            }
        
        # Calculate signal
        signal = self._calculate_signal(symbol, direction, predictions, market, is_personal)
        if signal is None:
            return {
                'success': False, 
                'reason': 'Signal calculation failed'
            }
        
        # Save signal
        try:
            signal_id = await self._save_signal(signal)
            logger.info(
                f"✅ Generated signal for {symbol}: {signal.direction.value} @ "
                f"${signal.entry_price:.2f} → ${signal.target_price_1:.2f} "
                f"(SL ${signal.stop_loss:.2f}, confidence: {signal.confidence_score}%)"
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
    print("SIGNAL GENERATOR (HOURLY-BASED)")
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
    print(f"  Signals auto-closed: {stats['signals_closed']}")
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