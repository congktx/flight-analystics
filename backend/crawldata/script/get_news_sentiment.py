# get_news_sentiment.py
import requests
import pandas as pd
from datetime import datetime

API_URL = "https://www.alphavantage.co/query"
API_KEY = "YOUR_ALPHA_VANTAGE_KEY"  # Thay bằng key của bạn

def fetch_news_sentiment():
    """
    Lấy dữ liệu tin tức thị trường kèm sentiment
    """
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": "",  # để trống lấy tất cả
        "topics": "",
        "apikey": API_KEY
    }

    resp = requests.get(API_URL, params=params)
    data = resp.json()
    feed = data.get("feed", [])

    df = pd.DataFrame(feed)
    today = datetime.today().strftime("%Y-%m-%d")
    filename = f"news_sentiment_{today}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} news articles to {filename}")

if __name__ == "__main__":
    fetch_news_sentiment()
