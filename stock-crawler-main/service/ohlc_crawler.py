import time

import requests

from database.mongodb import MongoDB

from utils.time_utils import timestamp_to_date

from config import PolygonConfig

mongodb = MongoDB()

def get_ohlc(ticker, from_timestamp, to_timestamp):
    from_date = timestamp_to_date(from_timestamp)
    to_date = timestamp_to_date(to_timestamp)
    url = f'https://api.polygon.io/v2/aggs/ticker/${ticker}/range/1/hour/${from_date}/${to_date}'
    
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "adjusted": True,
        "sort": "asc",
        "limit": 50000,
        "apiKey": PolygonConfig.API_KEY
    }
    
    try:
        response = requests.get(url, params=params, headers=headers).json()
        
        next_url = response['next_url'] or None
        
        if not response['results']:
            return response['errors']

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None
    
def ohlc_get_next_url(url):
    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers).json()
        next_url = response['next_url'] or None

        if not response['results']:
            return response['errors']

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None

def load_all_ohlc_to_db(list_ohlc, time_update):
    for ohlc in list_ohlc:
        document = {
            "_id": ohlc.get('ticker') + '_' + ohlc.get('t'),
            "ticker": ohlc.get('ticker'),
            "t": ohlc.get('t'),
            "o": ohlc.get('o'),
            "h": ohlc.get('h'),
            'l': ohlc.get('l'),
            'c': ohlc.get('c'),
            'v': ohlc.get('v'),
            "time_update": time_update
        }
    
        mongodb.upsert_space_ohlc(document)
        time.sleep(0.1)
        
def crawl_all_