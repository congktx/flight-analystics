import requests
from utils.config import *
from utils.utils import parse_date_to_timestamp
from utils.utils import save_info
from typing import List, Dict
from pprint import pprint

def get_market_status(date: str):
    params = {
        "apikey": AlphavantageConfig.API_KEY,
        "function": "MARKET_STATUS"
    }

    headers = {
        "Content-Type": "application/json"
    }

    url = AlphavantageConfig.URL

    try:
        res = requests.get(url, params=params, headers=headers).json()
        if res.get('markets'):
            markets = res["markets"]
            data = __add_time_stamp(date=date, data=markets)
            save_info(path=GlobalConfig.MARKET_DATA_PATH,
                      data=data, id=1)
    except Exception as e:
        print(f"Error fetching data: {e}")

def __add_time_stamp(date: str, data: List[Dict]):
    new_data = []
    for status in data:
        status["time_update"] = parse_date_to_timestamp(date=date)
        new_data.append(status)
    return new_data