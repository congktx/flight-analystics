# get_market_status.py
import requests
import pandas as pd
from datetime import datetime

API_URL = "https://www.alphavantage.co/query"
API_KEY = "YOUR_ALPHA_VANTAGE_KEY"  # Thay bằng key của bạn

def fetch_market_status():
    """
    Lấy trạng thái thị trường chứng khoán toàn cầu
    """
    params = {
        "function": "MARKET_STATUS",
        "apikey": API_KEY
    }
    resp = requests.get(API_URL, params=params)
    data = resp.json()

    # Chuyển sang DataFrame
    markets = data.get("markets", [])
    df = pd.DataFrame(markets)
    month = datetime.today().strftime("%Y-%m")
    filename = f"market_status_{month}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} markets to {filename}")

if __name__ == "__main__":
    fetch_market_status()
