from sqlalchemy import text

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS stocks (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(20) UNIQUE NOT NULL,
        company_name VARCHAR(255),
        sector VARCHAR(100),
        industry VARCHAR(100),
        currency VARCHAR(10) DEFAULT 'IDR',
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS technical_prices (
        id SERIAL PRIMARY KEY,
        stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
        date DATE NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION NOT NULL,
        adj_close DOUBLE PRECISION,
        volume BIGINT,
        data_source VARCHAR(50),
        UNIQUE(stock_id, date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS technical_indicators (
        id SERIAL PRIMARY KEY,
        stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
        date DATE NOT NULL,
        rsi DOUBLE PRECISION,
        macd DOUBLE PRECISION,
        macd_signal DOUBLE PRECISION,
        sma_20 DOUBLE PRECISION,
        sma_50 DOUBLE PRECISION,
        ema_20 DOUBLE PRECISION,
        bb_upper DOUBLE PRECISION,
        bb_lower DOUBLE PRECISION,
        bb_middle DOUBLE PRECISION,
        daily_return DOUBLE PRECISION,
        volatility_20 DOUBLE PRECISION,
        volume_sma_20 DOUBLE PRECISION,
        volume_ratio DOUBLE PRECISION,
        atr_14 DOUBLE PRECISION,
        stoch_rsi DOUBLE PRECISION,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(stock_id, date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS macro_economic (
        date DATE PRIMARY KEY,
        usd_idr DOUBLE PRECISION,
        ihsg DOUBLE PRECISION,
        gold_price DOUBLE PRECISION,
        oil_price DOUBLE PRECISION,
        macro_sentiment_score DOUBLE PRECISION DEFAULT 0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS news_sentiment (
        id SERIAL PRIMARY KEY,
        stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
        date DATE NOT NULL,
        sentiment_score DOUBLE PRECISION,
        news_count INTEGER,
        UNIQUE(stock_id, date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamental_quarterly (
        id SERIAL PRIMARY KEY,
        stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
        ticker VARCHAR(20),
        year INTEGER NOT NULL,
        quarter VARCHAR(10) NOT NULL,
        report_date DATE NOT NULL,
        revenue BIGINT,
        net_profit BIGINT,
        eps DOUBLE PRECISION,
        total_assets BIGINT,
        total_liabilities BIGINT,
        total_equity BIGINT,
        roe DOUBLE PRECISION,
        data_source VARCHAR(50),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(stock_id, year, quarter)
    );
    """
]

# UNIVERSAL SCHEMA MAP (For Auto-Healer)
SCHEMA_MAP = {
    "stocks": ["ticker", "company_name", "sector", "industry", "currency", "is_active"],
    "technical_prices": ["stock_id", "date", "open", "high", "low", "close", "adj_close", "volume", "data_source"],
    "technical_indicators": [
        "stock_id", "date", "rsi", "macd", "macd_signal", "sma_20", "sma_50", "ema_20", 
        "bb_upper", "bb_lower", "bb_middle", "daily_return", "volatility_20", 
        "volume_sma_20", "volume_ratio", "atr_14", "stoch_rsi"
    ],
    "macro_economic": ["date", "usd_idr", "ihsg", "gold_price", "oil_price", "macro_sentiment_score"],
    "news_sentiment": ["stock_id", "date", "sentiment_score", "news_count"],
    "fundamental_quarterly": [
        "stock_id", "ticker", "year", "quarter", "report_date", "revenue", 
        "net_profit", "eps", "total_assets", "total_liabilities", "total_equity", "roe"
    ]
}

def init_tables(engine):
    if engine is None: return
    print("\n[DB-HEALER] Starting Universal Schema Verification...")
    with engine.begin() as conn:
        # 1. Create missing tables
        for query in TABLES:
            conn.execute(text(query))
        
        # 2. Universal Column Healer (Simplified check)
        for table, cols in SCHEMA_MAP.items():
            for col in cols:
                try:
                    # In PostgreSQL, we can use a generic type for new columns in this auto-healer
                    # For a truly robust system, we use DOUBLE PRECISION as default for numeric
                    dtype = "DOUBLE PRECISION"
                    if col in ["id", "stock_id", "news_count", "year", "volume"]: dtype = "BIGINT"
                    if col in ["date", "report_date"]: dtype = "DATE"
                    
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype};"))
                except Exception:
                    pass
    print("[DB-HEALER] Schema is now up-to-date. ✅")
