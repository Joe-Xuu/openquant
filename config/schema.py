"""
================================================================================
CONFIGURATION SCHEMA — Validated Single Source of Truth (Pydantic v2)
================================================================================

Loads settings from config/settings.json and overlays environment variables
from .env, validating all invariants at startup (fail-fast).
================================================================================
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field, model_validator


class ExchangeConfig(BaseModel):
    name: str = "binance"
    market: Literal["spot", "futures"] = "spot"
    testnet: bool = True
    api_key: str = ""
    api_secret: str = ""
    testnet_api_key: str = ""
    testnet_api_secret: str = ""
    rest_base_url: str = "https://api.binance.com"
    testnet_rest_base_url: str = "https://testnet.binance.vision"
    ws_base_url: str = "wss://stream.binance.com:9443/ws"
    testnet_ws_base_url: str = "wss://testnet.binance.vision/ws"
    recv_window: int = Field(default=5000, ge=1000, le=60000)
    rate_limit_rps: float = Field(default=10.0, gt=0.0, le=50.0)


class TradingConfig(BaseModel):
    symbols: List[str] = Field(default_factory=lambda: ["DOGEUSDT"], min_length=1)
    base_quote_asset: str = "USDT"
    fallback_equity: float = Field(default=100.0, gt=0.0)
    grid_capital_pct: float = Field(default=0.45, gt=0.0, le=1.0)
    trend_capital_pct: float = Field(default=0.50, ge=0.0, le=1.0)
    leverage: float = Field(default=1.0, ge=1.0, le=3.0)
    margin_type: Literal["ISOLATED", "CROSSED"] = "ISOLATED"
    max_positions_per_symbol: int = Field(default=1, ge=1)
    max_total_positions: int = Field(default=5, ge=1)
    order_timeout_seconds: int = Field(default=30, ge=5)
    slippage_bps: float = Field(default=2.5, ge=0.0)

    @model_validator(mode="after")
    def validate_capital_allocation(self) -> TradingConfig:
        total = self.grid_capital_pct + self.trend_capital_pct
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"grid_capital_pct ({self.grid_capital_pct}) + "
                f"trend_capital_pct ({self.trend_capital_pct}) = {total:.2f} > 1.0"
            )
        return self


class CircuitBreakerConfig(BaseModel):
    consecutive_losses: int = Field(default=5, ge=1)
    volatility_spike_multiplier: float = Field(default=3.0, gt=1.0)
    cooldown_minutes: int = Field(default=30, ge=1)


class RiskConfig(BaseModel):
    max_drawdown_pct: float = Field(default=15.0, gt=0.0, le=100.0)
    max_daily_loss_pct: float = Field(default=5.0, gt=0.0, le=100.0)
    max_position_size_pct: float = Field(default=75.0, gt=0.0, le=100.0)
    max_leverage: float = Field(default=1.0, ge=1.0)
    max_exposure_pct: float = Field(default=100.0, gt=0.0, le=500.0)
    stop_loss_pct: float = Field(default=2.0, gt=0.0)
    trailing_stop_pct: float = Field(default=1.5, gt=0.0)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)


class RegimeDetectionConfig(BaseModel):
    lookback_periods: int = Field(default=100, ge=20)
    adx_threshold: float = Field(default=25.0, gt=0.0)
    volatility_window: int = Field(default=20, ge=5)
    volatility_percentile_low: float = Field(default=25.0, ge=0.0, le=100.0)
    volatility_percentile_high: float = Field(default=75.0, ge=0.0, le=100.0)


class MacroFilterConfig(BaseModel):
    enabled: bool = True
    interval: str = "1h"
    short_interval: str = "4h"
    ema_fast: int = Field(default=50, ge=5)
    ema_slow: int = Field(default=200, ge=20)
    pause_grid_in_bear: bool = True
    close_grid_in_bear: bool = True
    bear_exit_depth_pct: float = Field(default=6.0, ge=0.0, le=30.0)

    @model_validator(mode="after")
    def validate_macro_emas(self) -> MacroFilterConfig:
        if self.ema_fast >= self.ema_slow:
            raise ValueError(f"macro ema_fast ({self.ema_fast}) must be < ema_slow ({self.ema_slow})")
        return self


class GridTradingConfig(BaseModel):
    type: Literal["geometric", "arithmetic"] = "geometric"
    upper_bound_pct: float = Field(default=6.0, gt=0.0)
    lower_bound_pct: float = Field(default=6.0, gt=0.0)
    grid_levels: int = Field(default=5, ge=1, le=50)
    profit_per_grid_pct: float = Field(default=1.8, gt=0.2)
    rebalance_threshold_pct: float = Field(default=5.0, gt=0.0)
    stop_loss_pct: float = Field(default=8.0, gt=0.0)
    spot_mode: bool = True


class TrendFollowingConfig(BaseModel):
    ema_fast: int = Field(default=12, ge=2)
    ema_slow: int = Field(default=26, ge=3)
    macd_signal: int = Field(default=9, ge=2)
    atr_period: int = Field(default=14, ge=2)
    atr_multiplier: float = Field(default=2.5, gt=0.0)
    trailing_stop_atr_multiplier: float = Field(default=4.0, gt=0.0)
    risk_per_trade_pct: float = Field(default=2.5, gt=0.0, le=10.0)
    min_adx: float = Field(default=20.0, ge=0.0, le=100.0)
    require_macd_confirm: bool = True
    exit_on_ema_cross: bool = False
    require_ema200_rising: bool = True
    allow_short: bool = False

    @model_validator(mode="after")
    def validate_ema_order(self) -> TrendFollowingConfig:
        if self.ema_fast >= self.ema_slow:
            raise ValueError(f"ema_fast ({self.ema_fast}) must be < ema_slow ({self.ema_slow})")
        return self


class MultiFactorConfig(BaseModel):
    weights: Dict[str, float] = Field(
        default_factory=lambda: {
            "trend_strength": 0.30,
            "volatility_regime": 0.25,
            "momentum": 0.20,
            "volume_profile": 0.15,
            "market_microstructure": 0.10,
        }
    )

    @model_validator(mode="after")
    def validate_weights(self) -> MultiFactorConfig:
        total = sum(self.weights.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"multi_factor weights must sum to 1.0, got {total:.4f}")
        return self


class StrategyConfig(BaseModel):
    mode: str = "parallel_dual_engine"
    macro_filter: MacroFilterConfig = Field(default_factory=MacroFilterConfig)
    regime_detection: RegimeDetectionConfig = Field(default_factory=RegimeDetectionConfig)
    grid_trading: GridTradingConfig = Field(default_factory=GridTradingConfig)
    trend_following: TrendFollowingConfig = Field(default_factory=TrendFollowingConfig)
    multi_factor: MultiFactorConfig = Field(default_factory=MultiFactorConfig)


class DataConfig(BaseModel):
    kline_intervals: List[str] = Field(default_factory=lambda: ["5m", "15m", "1h"])
    primary_interval: str = "5m"
    trend_interval: str = "1h"
    max_klines_per_request: int = Field(default=500, ge=50, le=1000)
    ws_reconnect_delay_seconds: float = Field(default=5.0, ge=1.0)
    indicator_cache_ttl_seconds: int = Field(default=60, ge=1)

    @model_validator(mode="after")
    def ensure_intervals_subscribed(self) -> DataConfig:
        for req in (self.primary_interval, self.trend_interval):
            if req not in self.kline_intervals:
                self.kline_intervals.append(req)
        return self


class DatabaseConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str = "openquant"
    dbname: str = "openquant"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "logs/trading_system.log"
    rotation: str = "10 MB"
    retention: str = "30 days"
    json_format: bool = False


class AppConfig(BaseModel):
    exchange: ExchangeConfig = Field(default_factory=ExchangeConfig)
    trading: TradingConfig = Field(default_factory=TradingConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @model_validator(mode="after")
    def enforce_spot_rules(self) -> AppConfig:
        if self.exchange.market == "spot":
            self.strategy.trend_following.allow_short = False
            self.trading.leverage = 1.0
            self.risk.max_leverage = 1.0
        else:
            # Futures: align risk max_leverage & exposure ceilings with configured leverage
            self.risk.max_leverage = max(self.risk.max_leverage, self.trading.leverage)
            self.risk.max_exposure_pct = max(
                self.risk.max_exposure_pct, 100.0 * self.trading.leverage
            )
        if self.strategy.macro_filter.enabled:
            for iv in (self.strategy.macro_filter.interval, self.strategy.macro_filter.short_interval):
                if iv and iv not in self.data.kline_intervals:
                    self.data.kline_intervals.append(iv)
        return self


def load_app_config(config_path: str = "config/settings.json") -> AppConfig:
    """Load config/settings.json, overlay .env overrides, and validate."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw: Dict[str, Any] = json.load(f)

    # Apply environment variable overrides
    ex = raw.setdefault("exchange", {})
    if os.getenv("BINANCE_TESTNET") is not None:
        ex["testnet"] = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
    if os.getenv("BINANCE_MARKET") is not None:
        ex["market"] = os.getenv("BINANCE_MARKET", "spot").lower()

    tr = raw.setdefault("trading", {})
    if os.getenv("BINANCE_LEVERAGE") is not None:
        tr["leverage"] = float(os.getenv("BINANCE_LEVERAGE", "1.0"))
    if os.getenv("BINANCE_MARGIN_TYPE") is not None:
        tr["margin_type"] = os.getenv("BINANCE_MARGIN_TYPE", "ISOLATED").upper()

    if ex.get("market") == "futures":
        st = raw.setdefault("strategy", {}).setdefault("trend_following", {})
        if os.getenv("ALLOW_SHORT") is not None:
            st["allow_short"] = os.getenv("ALLOW_SHORT", "true").lower() == "true"
        elif "allow_short" not in st or st["allow_short"] is False:
            st["allow_short"] = True

    rk = raw.setdefault("risk", {})
    for key, env_key in (
        ("max_drawdown_pct", "MAX_DRAWDOWN_PCT"),
        ("max_daily_loss_pct", "MAX_DAILY_LOSS_PCT"),
        ("max_position_size_pct", "MAX_POSITION_SIZE_PCT"),
        ("max_exposure_pct", "MAX_EXPOSURE_PCT"),
    ):
        val = os.getenv(env_key)
        if val is not None:
            rk[key] = float(val)

    db = raw.setdefault("database", {})
    if os.getenv("PG_HOST"):
        db["host"] = os.getenv("PG_HOST")
    if os.getenv("PG_PORT"):
        db["port"] = int(os.getenv("PG_PORT"))
    if os.getenv("PG_USER"):
        db["user"] = os.getenv("PG_USER")
    if os.getenv("PG_DB"):
        db["dbname"] = os.getenv("PG_DB")

    return AppConfig.model_validate(raw)
