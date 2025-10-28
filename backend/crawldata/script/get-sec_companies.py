# get_sec_companies.py
import requests
import pandas as pd
from datetime import datetime

API_URL = "https://api.polygon.io/v3/reference/tickers"
API_KEY = "YOUR_POLYGON_API_KEY"  # Thay bằng key của bạn

def fetch_sec_companies(month: str):
    """
    Lấy danh sách công ty từ NYSE/NASDAQ theo tháng
    month: 'YYYY-MM' (vd: '2025-10')
    """
    params = {
        "market": "stocks",
        "locale": "us",
        "limit": 1000,
        "apiKey": API_KEY,
    }

    all_data = []
    page = 1
    while True:
        params["page"] = page
        resp = requests.get(API_URL, params=params)
        data = resp.json()
        if "results" not in data or not data["results"]:
            break
        all_data.extend(data["results"])
        page += 1

    df = pd.DataFrame(all_data)
    filename = f"sec_companies_{month}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} companies to {filename}")

if __name__ == "__main__":
    fetch_sec_companies(datetime.today().strftime("%Y-%m"))
