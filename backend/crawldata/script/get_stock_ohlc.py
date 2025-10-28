# get_stock_ohlc.py
import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "YOUR_POLYGON_API_KEY"  # Thay bằng key của bạn

def fetch_ohlc(ticker: str, start_date: str, end_date: str):
    """
    Lấy dữ liệu OHLC hàng ngày cho 1 cổ phiếu
    ticker: mã cổ phiếu (ví dụ: AAPL)
    start_date, end_date: 'YYYY-MM-DD'
    """
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    params = {"adjusted": "true", "sort": "asc", "apiKey": API_KEY}

    resp = requests.get(url, params=params)
    data = resp.json()
    results = data.get("results", [])

    df = pd.DataFrame(results)
    if not df.empty:
        df['t'] = pd.to_datetime(df['t'], unit='ms')  # timestamp -> datetime
        df.rename(columns={'o':'open', 'h':'high', 'l':'low', 'c':'close', 'v':'volume'}, inplace=True)

    filename = f"{ticker}_ohlc_{start_date}_to_{end_date}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} rows to {filename}")

if __name__ == "__main__":
    # Ví dụ: lấy dữ liệu 7 ngày gần nhất của AAPL
    end = datetime.today()
    start = end - timedelta(days=7)
    fetch_ohlc("AAPL", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
