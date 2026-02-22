import yfinance as yf
import pandas as pd
import time
import os
import json
from typing import Dict, Any, Optional

from sqlalchemy import text

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))


# FUNDAMENTAL COLLECTOR 

class FundamentalCollector:
    """
    Quarterly fundamental data collector (LONG-TERM SIGNAL).
    Source    : Yahoo Finance
    Frequency : Quarterly
    Purpose   : Long-term trend / trajectory
    """

    def __init__(self, engine):
        self.engine = engine
        self.config = self._load_config()

        self.years_back = self.config["data_collection"]["years_back"]
        self.delay = self.config["data_collection"]["request_delay"]
        self.q_cfg = self.config["fundamental"]["quarterly"]

        print("[OK] FundamentalCollector (quarterly-only) initialized")


    def _load_config(self) -> Dict[str, Any]:
        import yaml
        path = os.path.join(PROJECT_ROOT, "config", "settings.yaml")
        if not os.path.exists(path):
            raise FileNotFoundError("settings.yaml not found")

        with open(path, "r") as f:
            return yaml.safe_load(f)

    def load_tickers(self, market: str) -> list[str]:
        path = os.path.join(PROJECT_ROOT, "config", "tickers.json")
        if not os.path.exists(path):
            raise FileNotFoundError("tickers.json not found")

        with open(path, "r") as f:
            data = json.load(f)

        if market not in data:
            raise ValueError(f"Market '{market}' not found in tickers.json")

        return data[market]

    
    # DB
    
    def ensure_stock_exists(self, ticker: str) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT id FROM stocks WHERE ticker = :ticker"),
                {"ticker": ticker}
            ).fetchone()

            if result:
                return result[0]

            conn.execute(
                text("""
                    INSERT INTO stocks (ticker, is_active)
                    VALUES (:ticker, true)
                """),
                {"ticker": ticker}
            )

            return conn.execute(
                text("SELECT id FROM stocks WHERE ticker = :ticker"),
                {"ticker": ticker}
            ).fetchone()[0]

    @staticmethod
    def _extract(df: pd.DataFrame, key: str, col) -> Optional[float]:
        """
        SAFE extractor:
        - df kosong → None
        - key tidak ada → None
        - kolom quarter tidak ada → None
        - NaN → None
        """
        try:
            if df is None or df.empty:
                return None
            if key not in df.index:
                return None
            if col not in df.columns:
                return None

            val = df.loc[key, col]
            if pd.isna(val):
                return None

            return float(val)
        except Exception:
            return None

    
    # QUARTERLY COLLECTION
    
    def collect_quarterly(self, ticker: str) -> int:
        stock_id = self.ensure_stock_exists(ticker)

        time.sleep(self.delay)
        yf_stock = yf.Ticker(ticker)

        income = yf_stock.quarterly_financials
        balance = yf_stock.quarterly_balance_sheet

        if income is None or income.empty:
            print(f"[WARN] No quarterly income data for {ticker}")
            return 0

        max_quarters = min(self.years_back * 4, len(income.columns))
        quarters = list(income.columns)[:max_quarters]

        saved = 0
        skipped = 0

        with self.engine.begin() as conn:
            for q in quarters:
                q_date = pd.Timestamp(q)
                year = q_date.year
                quarter = f"Q{(q_date.month - 1) // 3 + 1}"

                revenue = self._extract(income, self.q_cfg["revenue_key"], q)
                net_profit = self._extract(income, self.q_cfg["net_profit_key"], q)
                eps = self._extract(income, self.q_cfg["eps_key"], q)

                assets = self._extract(balance, self.q_cfg["assets_key"], q)
                liabilities = self._extract(balance, self.q_cfg["liabilities_key"], q)

                # kalau semua fundamental inti kosong skip quarter
                if all(v is None for v in [revenue, net_profit, assets, liabilities]):
                    skipped += 1
                    continue

                if assets is not None and liabilities is not None:
                    equity = assets - liabilities
                    # Calculate ROE if possible (Net Profit / Equity)
                    roe = net_profit / equity if equity and net_profit is not None and equity != 0 else None
                else:
                    equity = None
                    roe = None

                conn.execute(
                    text("""
                        INSERT INTO fundamental_quarterly (
                            stock_id, ticker, year, quarter, report_date,
                            revenue, net_profit, eps,
                            total_assets, total_liabilities, total_equity, roe, data_source
                        )
                        VALUES (
                            :stock_id, :ticker, :year, :quarter, :report_date,
                            :revenue, :net_profit, :eps,
                            :assets, :liabilities, :equity, :roe,
                            'yahoo_finance'
                        )
                        ON CONFLICT (stock_id, year, quarter)
                        DO UPDATE SET
                            revenue = EXCLUDED.revenue,
                            net_profit = EXCLUDED.net_profit,
                            eps = EXCLUDED.eps,
                            total_assets = EXCLUDED.total_assets,
                            total_liabilities = EXCLUDED.total_liabilities,
                            total_equity = EXCLUDED.total_equity,
                            roe = EXCLUDED.roe,
                            updated_at = CURRENT_TIMESTAMP
                    """),
                    {
                        "stock_id": stock_id,
                        "ticker": ticker,
                        "year": year,
                        "quarter": quarter,
                        "report_date": q_date.date(),
                        "revenue": revenue,
                        "net_profit": net_profit,
                        "eps": eps,
                        "assets": assets,
                        "liabilities": liabilities,
                        "equity": equity,
                        "roe": roe
                    }
                )

                saved += 1

        print(f"[DONE] {ticker}: saved={saved}, skipped={skipped}")
        return saved



if __name__ == "__main__":
    import argparse
    from src.database.connection import engine

    parser = argparse.ArgumentParser("Quarterly Fundamental Collector")
    parser.add_argument("--ticker", help="Single ticker (e.g. BBCA.JK)")
    parser.add_argument("--market", help="Market key from tickers.json (e.g. indonesia)")
    args = parser.parse_args()

    collector = FundamentalCollector(engine)

    if args.ticker:
        tickers = [args.ticker]
    elif args.market:
        tickers = collector.load_tickers(args.market)
    else:
        raise ValueError("Use --ticker or --market")

    for t in tickers:
        collector.collect_quarterly(t)
