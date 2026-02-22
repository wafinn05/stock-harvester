import pandas as pd
import numpy as np
import os
import argparse
from sqlalchemy import text

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

from src.database.connection import engine


# TECHNICAL
SMA_20 = 20
SMA_50 = 50

RSI_PERIOD = 14

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2

VOLATILITY_PERIOD = 20
VOLUME_SMA_PERIOD = 20

#CALCULATION

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    close = df["close"]
    volume = df["volume"]

    # SMA
    df["sma_20"] = close.rolling(SMA_20, min_periods=SMA_20).mean()
    df["sma_50"] = close.rolling(SMA_50, min_periods=SMA_50).mean()

    # EMA
    df["ema_20"] = close.ewm(span=20, adjust=False).mean()

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/RSI_PERIOD, adjust=False).mean()

    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()

    df["macd"] = ema_fast - ema_slow
    df["macd_signal"] = df["macd"].ewm(span=MACD_SIGNAL, adjust=False).mean()

    #Bollinger Bands
    bb_mid = close.rolling(BOLLINGER_PERIOD, min_periods=BOLLINGER_PERIOD).mean()
    bb_std = close.rolling(BOLLINGER_PERIOD, min_periods=BOLLINGER_PERIOD).std()

    df["bb_middle"] = bb_mid
    df["bb_upper"] = bb_mid + (bb_std * BOLLINGER_STD)
    df["bb_lower"] = bb_mid - (bb_std * BOLLINGER_STD)

    # Returns & Volatility
    df["daily_return"] = close.pct_change()
    df["volatility_20"] = df["daily_return"].rolling(
        VOLATILITY_PERIOD,
        min_periods=VOLATILITY_PERIOD
    ).std()

    # Volume
    df["volume_sma_20"] = volume.rolling(
        VOLUME_SMA_PERIOD,
        min_periods=VOLUME_SMA_PERIOD
    ).mean()
    df["volume_ratio"] = volume / df["volume_sma_20"]

    # --- ADVANCED FEATURES (v2) ---
    


    # 2. ATR (Average True Range) - Volatility
    # TR = Max(High-Low, High-PrevClose, Low-PrevClose)
    prev_close = close.shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()

    # 3. Stochastic RSI
    # StochRSI = (RSI - MinRSI) / (MaxRSI - MinRSI)
    min_rsi = df["rsi"].rolling(14).min()
    max_rsi = df["rsi"].rolling(14).max()
    df["stoch_rsi"] = (df["rsi"] - min_rsi) / (max_rsi - min_rsi)

    return df

def update_indicators_for_ticker(ticker: str) -> bool:
    print(f"\n[TECH] Processing {ticker}")

    with engine.connect() as conn:
        stock = conn.execute(
            text("SELECT id FROM stocks WHERE ticker = :t"),
            {"t": ticker}
        ).fetchone()

        if not stock:
            print(f"[WARN] {ticker} not found")
            return False

        stock_id = stock[0]

        prices = conn.execute(
            text("""
                SELECT date, close, volume, high, low
                FROM technical_prices
                WHERE stock_id = :sid
                ORDER BY date
            """),
            {"sid": stock_id}
        ).fetchall()

    if len(prices) < SMA_50 + 10:
        print(f"[SKIP] Not enough data ({len(prices)})")
        return False

    df = pd.DataFrame(prices, columns=["date", "close", "volume", "high", "low"])
    df.set_index("date", inplace=True)

    df = calculate_indicators(df)

    saved, skipped = 0, 0

    with engine.begin() as conn:
        for date_idx, r in df.iterrows():

            if pd.isna(r["rsi"]) or pd.isna(r["sma_50"]):
                skipped += 1
                continue

            conn.execute(
                text("""
                    INSERT INTO technical_indicators (
                        stock_id, date,
                        rsi, macd, macd_signal,
                        sma_20, sma_50, ema_20,
                        bb_upper, bb_lower, bb_middle,
                        daily_return, volatility_20,
                        volume_sma_20, volume_ratio,
                        atr_14, stoch_rsi
                    )
                    VALUES (
                        :sid, :d,
                        :rsi, :macd, :macd_signal,
                        :sma20, :sma50, :ema20,
                        :bb_u, :bb_l, :bb_m,
                        :ret, :vol,
                        :vol_sma, :vol_ratio,
                        :atr, :stoch
                    )
                    ON CONFLICT (stock_id, date)
                    DO UPDATE SET
                        rsi = EXCLUDED.rsi,
                        macd = EXCLUDED.macd,
                        macd_signal = EXCLUDED.macd_signal,
                        sma_20 = EXCLUDED.sma_20,
                        sma_50 = EXCLUDED.sma_50,
                        ema_20 = EXCLUDED.ema_20,
                        bb_upper = EXCLUDED.bb_upper,
                        bb_lower = EXCLUDED.bb_lower,
                        bb_middle = EXCLUDED.bb_middle,
                        daily_return = EXCLUDED.daily_return,
                        volatility_20 = EXCLUDED.volatility_20,
                        volume_sma_20 = EXCLUDED.volume_sma_20,
                        volume_ratio = EXCLUDED.volume_ratio,
                        atr_14 = EXCLUDED.atr_14,
                        stoch_rsi = EXCLUDED.stoch_rsi,
                        updated_at = CURRENT_TIMESTAMP
                """),
                {
                    "sid": stock_id,
                    "d": date_idx,
                    "rsi": float(r["rsi"]),
                    "macd": float(r["macd"]),
                    "macd_signal": float(r["macd_signal"]),
                    "sma20": float(r["sma_20"]),
                    "sma50": float(r["sma_50"]),
                    "ema20": float(r["ema_20"]),
                    "bb_u": float(r["bb_upper"]),
                    "bb_l": float(r["bb_lower"]),
                    "bb_m": float(r["bb_middle"]),
                    "ret": float(r["daily_return"]),
                    "vol": float(r["volatility_20"]),
                    "vol_sma": float(r["volume_sma_20"]),
                    "vol_ratio": float(r["volume_ratio"]),
                    "atr": float(r["atr_14"]) if not pd.isna(r["atr_14"]) else None,
                    "stoch": float(r["stoch_rsi"]) if not pd.isna(r["stoch_rsi"]) else None,
                }
            )
            saved += 1

    print(f"[OK] {ticker}: saved={saved}, skipped={skipped}")
    return True


# MAIN
def main():
    parser = argparse.ArgumentParser(description="Technical Indicator Engine")
    parser.add_argument(
        "--ticker",
        type=str,
        help="Run indicator only for specific ticker (e.g. BBCA)"
    )

    args = parser.parse_args()

    print("\n" + "=" * 50)
    print("TECHNICAL INDICATORS ENGINE")
    print("=" * 50)

    if args.ticker:
        update_indicators_for_ticker(args.ticker.upper())
        return
    with engine.connect() as conn:
        tickers = conn.execute(
            text("SELECT ticker FROM stocks ORDER BY ticker")
        ).fetchall()

    for (ticker,) in tickers:
        update_indicators_for_ticker(ticker)


if __name__ == "__main__":
    main()
