import yfinance as yf
import pandas as pd
from sqlalchemy import text
import time
import json
import os

from src.database.connection import engine


# CONFIG


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
DEFAULT_PERIOD = "2y"
REQUEST_DELAY = 1.5


# UTILS

def load_tickers(region="indonesia"):
    path = os.path.join(CONFIG_DIR, "tickers.json")
    with open(path, "r") as f:
        data = json.load(f)
    return data.get(region, [])


def get_or_create_stock(ticker: str) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            text("SELECT id FROM stocks WHERE ticker = :t"),
            {"t": ticker}
        ).fetchone()

        if res:
            return res[0]

        try:
            info = yf.Ticker(ticker).info
        except Exception:
            info = {}

        result = conn.execute(
            text("""
                INSERT INTO stocks (ticker, company_name, sector, industry, currency)
                VALUES (:ticker, :name, :sector, :industry, :currency)
                RETURNING id
            """),
            {
                "ticker": ticker,
                "name": info.get("longName", ticker),
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "currency": info.get("currency", "IDR"),
            }
        )
        return result.fetchone()[0]

# CORE LOGIC

def fetch_and_store(ticker: str, period: str = DEFAULT_PERIOD):
    print(f"\n{ticker} | period={period}")

    time.sleep(REQUEST_DELAY)

    stock_id = get_or_create_stock(ticker)

    df = yf.download(
        ticker,
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        print("No data returned")
        return

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.reset_index(inplace=True)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df.columns = [c.replace(" ", "_") for c in df.columns]

    inserted = 0

    with engine.begin() as conn:
        for r in df.itertuples(index=False):

            trade_date = r.Date

            exists = conn.execute(
                text("""
                    SELECT 1 FROM technical_prices
                    WHERE stock_id = :sid AND date = :d
                """),
                {"sid": stock_id, "d": trade_date}
            ).fetchone()

            if exists:
                continue

            conn.execute(
                text("""
                    INSERT INTO technical_prices
                    (stock_id, date, open, high, low, close, adj_close, volume, data_source)
                    VALUES
                    (:sid, :d, :o, :h, :l, :c, :ac, :v, 'yahoo_finance')
                """),
                {
                    "sid": stock_id,
                    "d": trade_date,
                    "o": float(r.Open),
                    "h": float(r.High),
                    "l": float(r.Low),
                    "c": float(r.Close),
                    "ac": float(r.Adj_Close),
                    "v": int(r.Volume),
                }
            )
            inserted += 1

    print(f"inserted {inserted} rows")


# CLI

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("Yahoo Technical Price Collector")
    parser.add_argument("--ticker", type=str, help="Single ticker")
    parser.add_argument("--period", type=str, default=DEFAULT_PERIOD)
    parser.add_argument("--limit", type=int, default=0)

    args = parser.parse_args()

    if args.ticker:
        fetch_and_store(args.ticker, args.period)
    else:
        tickers = load_tickers("indonesia")
        if args.limit > 0:
            tickers = tickers[:args.limit]

        for i, item in enumerate(tickers, 1):
            # Support both string and dict formats
            if isinstance(item, dict):
                t = item["ticker"]
            else:
                t = item
            
            print(f"[{i}/{len(tickers)}]")
            fetch_and_store(t, args.period)
