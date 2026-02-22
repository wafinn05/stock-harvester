import os
import sys
import logging
import argparse
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

def run_daily_mining(mode="all", batch_idx=0, total_batches=1, run_init=False):
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    now_jakarta = datetime.now(jakarta_tz)
    logger.info(f"=== DAILY MINING SESSION ({mode.upper()}) | Batch {batch_idx+1}/{total_batches} ===")
    
    try:
        # 0. Verifikasi/Inisialisasi Tabel (Hanya jika --init dipanggil)
        if run_init:
            logger.info("[INIT] Melakukan verifikasi struktur database (DDL)...")
            init_tables(engine)
        
        # 1. Koleksi Data Makro
        if mode in ["all", "macro"]:
            logger.info("[STEP 1] Collecting Macroeconomic Data & Global Sentiment...")
            collect_macro()
            collect_macro_sentiment()
        
        # 2. Koleksi Data Per Saham
        if mode in ["all", "stocks"]:
            if mode == "stocks":
                logger.info("[WAIT] Memberi waktu 30 detik agar Macro Miner menyelesaikan urusan schema...")
                import time
                time.sleep(30)
            
            logger.info("[STEP 2] Collecting Ticker Specific Data...")
            all_tickers_info = load_tickers("indonesia")
            
            # --- SHARDING LOGIC ---
            if total_batches > 1:
                # Bagi total saham ke dalam beberapa batch
                # Contoh: 31 saham, 2 batch -> Batch 0 (16 saham), Batch 1 (15 saham)
                import math
                chunk_size = math.ceil(len(all_tickers_info) / total_batches)
                start_idx = batch_idx * chunk_size
                end_idx = start_idx + chunk_size
                tickers_to_process = all_tickers_info[start_idx:end_idx]
                logger.info(f"Sharding Active: Processing {len(tickers_to_process)} tickers (Range: {start_idx}-{end_idx})")
            else:
                tickers_to_process = all_tickers_info
            
            fundamental_collector = FundamentalCollector(engine)
            
            for i, t_info in enumerate(tickers_to_process, 1):
                ticker = t_info["ticker"]
                try:
                    logger.info(f"[{i}/{len(tickers_to_process)}] --- Processing {ticker} ---")
                    
                    # A. Tarik Harga Terakhir
                    fetch_and_store(ticker, period="7d")
                    
                    # B. Hitung Indikator Teknikal
                    update_indicators_for_ticker(ticker)
                    
                    # C. Tarik Sentimen Berita
                    collect_sentiment(target_ticker=ticker)
                    
                    # D. Cek Fundamental (Quarterly)
                    fundamental_collector.collect_quarterly(ticker)
                    
                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {str(e)}")
                    continue

        logger.info(f"=== {mode.upper()} MINING SESSION COMPLETED ===")

    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        logger.critical(f"=== MINING SESSION CRASHED ({mode}) ===")
        print(error_msg)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Forecasting Daily Miner with Sharding Support")
    parser.add_argument("--mode", default="all", choices=["all", "macro", "stocks"], help="Mining mode")
    parser.add_argument("--batch", type=int, default=0, help="Batch index (0-based)")
    parser.add_argument("--total-batches", type=int, default=1, help="Total number of batches")
    parser.add_argument("--init", action="store_true", help="Inisialisasi/Heal database schema (DDL)")
    
    args, unknown = parser.parse_known_args() # Use parse_known_args to avoid issues with extra flags
    
    # Handle the case where someone might use -m instead of --mode if we chose to
    # but for now we'll stick to clear arguments.
    
    # Force flush output for GitHub Actions Logs
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    
    run_daily_mining(
        mode=args.mode, 
        batch_idx=args.batch, 
        total_batches=args.total_batches,
        run_init=args.init
    )
