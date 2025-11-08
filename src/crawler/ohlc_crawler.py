from pprint import pprint
from utils.utils import *
from utils.config import *
import requests
import time

def get_ohlc_data(from_date: str, to_date: str):
    list_tickers, id = get_company_labels(task_owner="Loi")
    id = 1
    for idx, ticker in enumerate(list_tickers):
        print(f"{idx+1} "+"="*15+f"{ticker}"+"="*15)
        id = get_ohlc_data_by_ticker(ticker=ticker,
                                     from_date=from_date,
                                     to_date=to_date,
                                     id=id)
        


def get_ohlc_data_by_ticker(ticker: str, from_date: str, to_date: str, id: int)-> int:
    time.sleep(12)
    url = f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/hour/{from_date}/{to_date}"
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": MassiveConfig.API_KEY
    }

    try:
        res = requests.get(url=url, headers=headers, params=params).json()
        res = dict(res)
        results = res["results"]
        results = adjust_data(data=results,
                              ticker=ticker)
        next_url = res.get("next_url")
        save_info(path=GlobalConfig.OHLC_DATA_PATH,
                  data=results,
                  id=id)
        if next_url != None:
            id = get_ohlc_data_next_url(ticker=ticker,
                                        url=next_url, 
                                        id=id+1)
    except Exception as e:
        print(f"Error while fetching data: {e} -> {id}")

    return id+1

def get_ohlc_data_next_url(ticker: str, url: str, id: int)-> int:
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": MassiveConfig.API_KEY
    }
    try:
        while url != None:
            time.sleep(12)
            res = requests.get(params=params, headers=headers, url=url).json()
            results = res.get("results")
            results = adjust_data(data=results,
                              ticker=ticker)
            url = res.get("next_url")
            save_info(path=GlobalConfig.OHLC_DATA_PATH,
                      data=results,
                      id=id)
            if url != None:
                id+=1
            else:
                url=None
    except Exception as e:
        print(f"Error while fetching data: {e} -> {id}")
    
    return id

def adjust_data(data: List[Dict], ticker: str):
    results = []
    for inf in data:
        inf["ticker"] = ticker
        results.append(inf)
    return results