import feedparser
import pandas as pd
from datetime import datetime
from src.database.connection import engine
from sqlalchemy import text
import urllib.parse
import json
import os

# Simple Indonesian Sentiment Dictionary
POSITIVE_WORDS = [
    "naik", "melonjak", "tumbuh", "laba", "untung", "dividen", "bullish", 
    "menguat", "positif", "rekor", "tertinggi", "buy", "akumulasi", "kinerja bagus"
]
NEGATIVE_WORDS = [
    "turun", "anjlok", "rugi", "merugi", "bearish", "melemah", "negatif", 
    "terendah", "sell", "jual", "koreksi", "gagal", "bangkrut", "utang"
]

from src.modeling.indobert import get_engine

# Initialize AI Engine (Lazy Load)
ai_engine = None

def get_sentiment_score(text):
    global ai_engine
    try:
        if ai_engine is None:
            ai_engine = get_engine()
        return ai_engine.predict(text)
    except Exception as e:
        print(f"AI Failure: {e}, falling back to keywords")
        text = text.lower()
        score = 0
        for w in POSITIVE_WORDS:
            if w in text: score += 1
        for w in NEGATIVE_WORDS:
            if w in text: score -= 1
        return max(min(score, 1.0), -1.0)

def collect_sentiment(target_ticker=None):
    print(f"Collecting Sentiment...")
    
    with engine.connect() as conn:
        # Get Tickers
        if target_ticker:
            stocks = conn.execute(text("SELECT id, ticker FROM stocks WHERE ticker = :t"), {"t": target_ticker}).fetchall()
        else:
            stocks = conn.execute(text("SELECT id, ticker FROM stocks")).fetchall()
        
        # Load Tickers Config
        CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "config", "tickers.json")
        
        ticker_map = {}
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                ticker_config = json.load(f)
            ticker_map = {item["ticker"]: item["name"] for item in ticker_config.get("indonesia", [])}
        
        today_date = datetime.now().date()
        print(f"Processing {len(stocks)} stocks...")
        
        for stock_id, ticker_raw in stocks:
            ticker_clean = ticker_raw.split(".")[0]
            company_name = ticker_map.get(ticker_raw, ticker_clean)
            
            query = f'"{company_name}" OR "{ticker_clean}"'
            encoded_query = urllib.parse.quote(query)
            
            print(f"  Searching: {query}")
            rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=id-ID&gl=ID&ceid=ID:id"
            
            feed = feedparser.parse(rss_url)
            total_score = 0
            count = 0
            
            for entry in feed.entries[:20]:
                title = entry.title
                score = get_sentiment_score(title)
                total_score += score
                count += 1
            
            final_score = 0
            if count > 0:
                avg = total_score / count
                final_score = max(min(avg, 1.0), -1.0)
                
            print(f"  {ticker_clean}: {count} news, Score: {final_score:.2f}")
            
            # Upsert
            conn.execute(text("""
                INSERT INTO news_sentiment (stock_id, date, sentiment_score, news_count)
                VALUES (:sid, :d, :s, :c)
                ON CONFLICT (stock_id, date) DO UPDATE 
                SET sentiment_score = EXCLUDED.sentiment_score, news_count = EXCLUDED.news_count;
            """), {"sid": stock_id, "d": today_date, "s": final_score, "c": count})
            conn.commit()

    print("Sentiment Collection Complete!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Specific ticker (e.g. ASII.JK)")
    args = parser.parse_args()
    collect_sentiment(target_ticker=args.ticker)
