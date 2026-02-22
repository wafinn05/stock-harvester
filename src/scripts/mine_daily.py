import os
import sys
import logging
from datetime import datetime
import pytz

# Tambahkan project root ke sys.path agar bisa import src
sys.path.append(os.getcwd())

from src.collectors.prices import load_tickers, fetch_and_store
from src.collectors.macro import collect_macro
from src.collectors.sentiment import collect_sentiment
from src.collectors.macro_sentiment import collect_macro_sentiment
from src.collectors.fundamental import FundamentalCollector
from src.features.technical import update_indicators_for_ticker
from src.database.connection import engine
from src.database.schema import init_tables

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_daily_mining():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    now_jakarta = datetime.now(jakarta_tz)
    logger.info(f"=== STARTING DAILY MINING SESSION ({now_jakarta.strftime('%Y-%m-%d %H:%M:%S')}) ===")

    try:
        # 0. Verifikasi/Inisialisasi Tabel
        logger.info("[STEP 0] Verifying database schema...")
        init_tables(engine)
        
        # 1. Koleksi Data Makro (IHSG, Gold, USD, Oil)
        logger.info("[STEP 1] Collecting Macroeconomic Data...")
        collect_macro()
        collect_macro_sentiment()
        
        # 2. Koleksi Data Per Saham
        logger.info("[STEP 2] Collecting Ticker Specific Data...")
        # Load tickers from JSON master list
        all_tickers_info = load_tickers("indonesia")
        
        # Fundamental harian (Hanya cek apakah perlu update)
        fundamental_collector = FundamentalCollector(engine)
        
        for t_info in all_tickers_info:
            ticker = t_info["ticker"]
            try:
                logger.info(f"--- Processing {ticker} ---")
                
                # A. Tarik Harga Terakhir (Period 7d sudah cukup untuk update harian)
                fetch_and_store(ticker, period="7d")
                
                # B. Hitung Indikator Teknikal (RSI, MACD, dll)
                update_indicators_for_ticker(ticker)
                
                # C. Tarik Sentimen Berita
                collect_sentiment(target_ticker=ticker)
                
                # D. Cek Fundamental (Quarterly)
                fundamental_collector.collect_quarterly(ticker)
                
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {str(e)}")
                continue

        logger.info("=== DAILY MINING SESSION COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        logger.critical(f"Mining session FAILED: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_daily_mining()
