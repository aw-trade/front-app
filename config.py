import os

class Config:
    ORCH_API_BASE_URL = os.getenv("ORCH_API_BASE_URL", "http://localhost:8000")
    
    DEFAULT_ALGO_CONFIG = {
        "order-book-algo": {
            "IMBALANCE_THRESHOLD": 0.6,
            "MIN_VOLUME_THRESHOLD": 10.0,
            "LOOKBACK_PERIODS": 5,
            "SIGNAL_COOLDOWN_MS": 100
        },
        "rsi-algo": {
            "RSI_PERIOD": 14,
            "RSI_OVERBOUGHT": 70,
            "RSI_OVERSOLD": 30,
            "SIGNAL_COOLDOWN_MS": 100
        }
    }
    
    DEFAULT_SIMULATOR_CONFIG = {
        "INITIAL_CAPITAL": 100000.0,
        "POSITION_SIZE_PCT": 0.05,
        "MAX_POSITION_SIZE": 10000.0,
        "TRADING_FEE_PCT": 0.001,
        "MIN_CONFIDENCE": 0.3,
        "ENABLE_SHORTING": True,
        "STATS_INTERVAL_SECS": 30
    }
    
    ALGORITHMS = ["order-book-algo", "rsi-algo"]
    
    DEFAULT_DURATION_SECONDS = 300