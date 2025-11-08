import requests
from utils.utils import MassiveConfig
from utils.config import *
from utils.utils import save_info
from pprint import pprint
import time
from typing import List, Dict

def get_company_infos(date: str, exchange: str):
    url = "https://api.massive.com/v3/reference/tickers"

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "apiKey":MassiveConfig.API_KEY,
        "exchange": exchange,
        "date": date,
        "active":"true",
        "order":"asc",
        "limit":"1000",
        "sort":"ticker",
        "market":"stocks"
    }

    try:
        res = requests.get(url=url, params=params, headers=headers).json()
        results = res["results"]
        results = adjust_data(data=results,
                              date=date)
        next_url = res.get("next_url")
        save_info(path=GlobalConfig.COMPANY_DATA_PATH,
                  data=results,
                  id=1)
        if next_url:
            cursor = next_url.split("cursor=")[-1]
            get_next_infos(cursor=cursor, date=date, id=2)
    except Exception as e:
        print(f"Error while fetching data: {e} -> {1}")

def get_next_infos(cursor: str, date: str, id: int):
    url = "https://api.massive.com/v3/reference/tickers"

    headers = {
        "Content-Type": "application/json"
    }
    params = {
        "cursor":cursor,
        "apiKey":MassiveConfig.API_KEY
    }

    try:
        while cursor != None:
            time.sleep(12)
            params["cursor"] = cursor
            res = requests.get(params=params, headers=headers, url=url).json()
            results = res["results"]
            adjust_data(data=results, date=date)
            next_url = res.get("next_url")
            save_info(path=GlobalConfig.COMPANY_DATA_PATH,
                      data=results,
                      id=id)
            if next_url:
                id += 1
                cursor = next_url.split("cursor=")[-1]
            else:
                cursor=None
    except Exception as e:
        print(f"Error while fetching data: {e} -> {id}")

def adjust_data(data: List[Dict], date: str):
    results = []
    for inf in data:
        inf["update_time"] = date
        results.append(inf)
    return results
