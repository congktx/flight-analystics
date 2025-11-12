import time

import requests

from database.mongodb import MongoDB

from utils.time_utils import timestamp_to_date

from config import PolygonConfig

from utils.json_file import load_json_file, save_json_file


mongodb = MongoDB()


def get_ohlc(ticker, from_timestamp, to_timestamp):
    from_date = timestamp_to_date(from_timestamp)
    to_date = timestamp_to_date(to_timestamp)
    url = f'https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/hour/{from_date}/{to_date}'

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

        next_url = response.get('next_url')

        if not response.get('results'):
            return response.get('errors'), None

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None


def ohlc_get_next_url(url):
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "apiKey": PolygonConfig.API_KEY
    }

    try:
        response = requests.get(url=url, headers=headers, params=params).json()

        next_url = response.get('next_url')

        if not response.get('results'):
            return response.get('errors'), None

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None


def load_all_ohlc_to_db(ticker, list_ohlc, time_update):
    list_documents = []
    for ohlc in list_ohlc:
        document = {
            "_id": ticker + '_' + str(ohlc.get('t')),
            "ticker": ticker,
            "t": ohlc.get('t'),
            "o": ohlc.get('o'),
            "h": ohlc.get('h'),
            'l': ohlc.get('l'),
            'c': ohlc.get('c'),
            'v': ohlc.get('v'),
            "time_update": time_update
        }

        list_documents.append(document)

    mongodb.upsert_space_many_ohlc(list_documents)
    time.sleep(0.1)


def crawl_all_ohlc(from_timestamp, to_timestamp, time_update):
    # timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    # filter = {
    #     "time_update": timestamp
    # }

    # list_company_infos = list(mongodb.find_documets(mongodb._company_infos, filter))
    # tickers = list(map(lambda x: x.get('ticker'), list_company_infos))
    dict = load_json_file('./tmp/division_of_labor.json')
    tickers = dict.get('Thinh')

    last_ticker = mongodb.find_last_timestamp(mongodb._OHLC)
    is_start_crawl = False

    for index, ticker in enumerate(tickers):
        if last_ticker == 'None' or last_ticker == ticker:
            is_start_crawl = True

        if not is_start_crawl:
            continue

        # number_document = mongodb._OHLC.count_documents({
        #     'ticker': ticker
        # })
        # time.sleep(0.1)

        # if number_document: continue

        print(index+1, ticker)

        mongodb.upsert_last_completed_timestamp(mongodb._OHLC, ticker)
        list_ohlc, next_url = get_ohlc(ticker, from_timestamp, to_timestamp)
        if isinstance(list_ohlc, list):
            load_all_ohlc_to_db(ticker, list_ohlc, time_update)

        time.sleep(15)

        while next_url:
            list_ohlc, next_url = ohlc_get_next_url(next_url)
            if isinstance(list_ohlc, list):
                load_all_ohlc_to_db(ticker, list_ohlc, time_update)
            time.sleep(15)
