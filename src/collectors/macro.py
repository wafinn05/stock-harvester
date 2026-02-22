import yfinance as yf
import pandas as pd
from src.database.connection import engine
from sqlalchemy import text

MACRO_TICKERS = {
    "USDIDR=X": "usd_idr",
    "^JKSE": "ihsg",
    "GC=F": "gold_price",
    "CL=F": "oil_price"
}

def collect_macro():
    print("Fetching Macro Data (Currencies, Indices, Commodities)...")
    
    dfs = []
    for ticker, col_name in MACRO_TICKERS.items():
        print(f"  - {ticker} -> {col_name}")
        try:
            hist = yf.Ticker(ticker).history(start="2015-01-01")
            if hist.empty: continue
            
            # Reset index to get 'Date' column and rename Close to col_name
            temp_df = hist[['Close']].reset_index()
            temp_df['Date'] = temp_df['Date'].dt.date
            temp_df = temp_df.rename(columns={'Close': col_name})
            dfs.append(temp_df)
        except Exception as e:
            print(f"    Error fetching {ticker}: {e}")

    # Merge all DataFrames
    from functools import reduce
    if not dfs:
        print("No macro data fetched.")
        return

    df_merged = reduce(lambda left, right: pd.merge(left, right, on='Date', how='outer'), dfs)
    df_merged = df_merged.sort_values('Date').ffill().bfill().dropna()
    
    print(f"Saving {len(df_merged)} macro records to DB...")
    
    query = text("""
        INSERT INTO macro_economic (date, usd_idr, ihsg, gold_price, oil_price)
        VALUES (:date, :usd_idr, :ihsg, :gold_price, :oil_price)
        ON CONFLICT (date) DO UPDATE 
        SET 
            usd_idr = EXCLUDED.usd_idr, 
            ihsg = EXCLUDED.ihsg,
            gold_price = EXCLUDED.gold_price,
            oil_price = EXCLUDED.oil_price;
    """)
    
    try:
        with engine.connect() as conn:
            # Batch execution using SQLAlchemy
            data = [
                {
                    "date": row['Date'], 
                    "usd_idr": row.get('usd_idr'), 
                    "ihsg": row.get('ihsg'), 
                    "gold_price": row.get('gold_price'), 
                    "oil_price": row.get('oil_price')
                } 
                for _, row in df_merged.iterrows()
            ]
            conn.execute(query, data)
            conn.commit()
    except Exception as e:
        print(f"Error saving macro data: {e}")
        
    print("Macro Data Collection Complete!")
    print("Macro Data Collection Complete!")

if __name__ == "__main__":
    collect_macro()
