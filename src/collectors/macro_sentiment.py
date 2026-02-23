import feedparser
import urllib.parse
from datetime import datetime, date
from src.database.connection import get_db_connection
from src.modeling.indobert import get_engine

KEYWORDS = [
    "Ekonomi Indonesia", 
    "Kurs Rupiah", 
    "IHSG", 
    "Inflasi Indonesia", 
    "Suku Bunga BI"
]

def collect_macro_sentiment():
    print("Collecting Macro Economic Sentiment...")
    ai_engine = get_engine()
    
    all_headlines = []
    
    for kw in KEYWORDS:
        query = urllib.parse.quote(kw)
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=id-ID&gl=ID&ceid=ID:id"
        print(f"  Searching: {kw}")
        
        feed = feedparser.parse(rss_url)
        for entry in feed.entries[:10]: # Top 10 per keyword
            all_headlines.append(entry.title)
            
    # Batch Prediction (TURBO MODE)
    count = len(all_headlines)
    if count > 0:
        print(f"  [TURBO] Menganalisis {count} headline secara batch...")
        scores = ai_engine.predict_batch(all_headlines)
        final_score = max(min(sum(scores) / count, 1.0), -1.0)
    else:
        final_score = 0
        
    print(f"\nFinal Macro Sentiment Score: {final_score:.4f} (from {count} headlines)")
    
    # Save to DB
    conn = get_db_connection()
    cur = conn.cursor()
    today = date.today()
    
    # Upsert
    cur.execute("""
        INSERT INTO macro_economic (date, macro_sentiment_score)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE
        SET macro_sentiment_score = EXCLUDED.macro_sentiment_score;
    """, (today, final_score))
    
    conn.commit()
    conn.close()
    print("Macro Sentiment Saved to DB!")

if __name__ == "__main__":
    collect_macro_sentiment()
