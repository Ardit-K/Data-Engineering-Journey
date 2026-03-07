import yfinance as yf
import pandas as pd
from typing import List, Dict

def fetch_market_data(tickers: List[str]) -> List[Dict]:
    """
    Fetches the last 5 days of closing prices for a list of tickers.
    Returns a list of dictionaries for your pipeline to process.
    """
    raw_results = []
    
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        # Get ticker info
        stock = yf.Ticker(ticker)
        
        # Get historical market data (last 5 days)
        hist = stock.history(period="5d")
        
        # Extract metadata for your dim_stocks table
        # Note: In production, you'd handle the case where info is missing
        info = stock.info
        company_name = info.get('longName', 'Unknown')
        sector = info.get('sector', 'Unknown')

        # Convert the pandas dataframe into a list of dictionaries for your Fact table
        for date, row in hist.iterrows():
            raw_results.append({
                "ticker": ticker,
                "company_name": company_name,
                "sector": sector,
                "date": date.strftime('%Y-%m-%d'),
                "close": row['Close'],
                "volume": int(row['Volume'])
            })
            
    return raw_results

# --- TEST IT ---
if __name__ == "__main__":
    symbols = ["AAPL", "MSFT", "GOOGL"]
    market_data = fetch_market_data(symbols)
    
    # Print the first item to see the structure
    print("\nSample Data Point:")
    print(market_data[0])