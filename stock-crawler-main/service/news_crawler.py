import json
import os
import requests

from config import AlphavantageConfig, AssignedCompaniesConfig

from database.mongodb import MongoDB

from utils.utils import text_to_hash

from utils.time_utils import timestamp_to_YYYYMMDDTHHMM

import time

mongodb = MongoDB()

def get_news_sentiment(tickers, from_timestamp, to_timestamp):
    url = 'https://www.alphavantage.co/query'
    
    params = {
        "function": 'NEWS_SENTIMENT',
        "tickers": tickers,
        "apikey": AlphavantageConfig.API_KEY,
        "limit": 1000
    }
    
    if from_timestamp:
        params.update({
            "time_from": timestamp_to_YYYYMMDDTHHMM(from_timestamp),
        })
        
    if to_timestamp:
        params.update({
            "time_to": timestamp_to_YYYYMMDDTHHMM(to_timestamp),
        })
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers).json()
        if not response.get('feed'):
            print(response)
            return []
        
        list_news = []
        for new in response.get('feed'):
            new.update({
                "sentiment_score_definition": response.get('sentiment_score_definition'),
                "relevance_score_definition": response.get('relevance_score_definition')
            })
            list_news.append(new)
        return list_news
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def load_all_news_sentiment_to_db(list_news, time_update):
    for news in list_news:
        document = {
            "_id": text_to_hash(news.get('title') + '_' + news.get('url')) + '_' + str(time_update),
            "title": news.get('title'),
            "url": news.get('url'),
            "time_published": news.get('time_published'),
            "authors": news.get('authors'),
            "summary": news.get('summary'),
            "source": news.get('source'),
            "source_domain": news.get('source_domain'),
            "topics": news.get('topics'),
            "overall_sentiment_score": news.get('overall_sentiment_score'),
            "overall_sentiment_label": news.get('overall_sentiment_label'),
            "ticker_sentiment": news.get('ticker_sentiment'),
            "sentiment_score_definition": news.get('sentiment_score_definition'),
            "relevance_score_definition": news.get('relevance_score_definition'),
            "time_update": time_update
        }
        
        mongodb.upsert_space_news(document)
        
def crawl_news_sentiment(from_timestamp, to_timestamp, time_update):
    # timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    filter = {
        "time_update": time_update
    }
    list_company_infos = list(mongodb.find_documents(mongodb._company_infos, filter))
    tickers = list(map(lambda x: x.get('ticker'), list_company_infos))

    for ticker in tickers:
        list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
        print(ticker, len(list_news))
        load_all_news_sentiment_to_db(list_news, time_update)    
        
        time.sleep(12)
        
def crawl_assigned_news_sentiment(from_timestamp, to_timestamp, time_update):
    # timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    filter = {
        "time_update": time_update
    }
    list_company_infos = list(mongodb.find_documents(mongodb._company_infos, filter))
    tickers = list(map(lambda x: x.get('ticker'), list_company_infos))

    for ticker in tickers:
        list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
        print(ticker, len(list_news))
        load_all_news_sentiment_to_db(list_news, time_update)    
        
        time.sleep(12)
        
def crawl_assigned_companies_ohlc(from_timestamp, to_timestamp, time_update):
    # load division_of_labor.json from project root
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dov_path = os.path.join(root, "division_of_labor.json")
    
    try:
        with open(dov_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            tickers = data.get(AssignedCompaniesConfig.ASSIGNED_COMPANIES, [])
            last_index = data.get("ticker_index_finished", {}).get("Long", {}).get("ohlc", -1)
            print("tickers counted:", len(tickers))
    except Exception as e:
        print(f"Error loading division_of_labor.json: {e}")
        return
        tickers = []
    
    for index in range(last_index + 1, len(tickers)):
        ticker = tickers[index]
        earliest_ohlc = get_earliest_ohlc(ticker)
        if earliest_ohlc:
            easrliest_timestamp = earliest_ohlc.get('t') / 1000 + 3600  # plus one hour
            print(f"Start crawling OHLC for ticker {ticker} from {timestamp_to_YYYYMMDDTHH(from_timestamp)}")
            load_ohlc_to_db(ticker, from_timestamp, easrliest_timestamp, time_update)
        latest_ohlc = get_latest_ohlc(ticker)
        if latest_ohlc:
            latest_timestamp = latest_ohlc.get('t') / 1000 + 3600  # plus one hour
            print(f"Continue crawling OHLC for ticker {ticker} from {timestamp_to_YYYYMMDDTHH(latest_timestamp)}")
            load_ohlc_to_db(ticker, latest_timestamp, to_timestamp, time_update)
        else:
            print(f"Start crawling OHLC for ticker{ticker} from {timestamp_to_YYYYMMDDTHH(from_timestamp)}")
            load_ohlc_to_db(ticker, from_timestamp, to_timestamp, time_update)
        print(f"Completed crawling OHLC data for ticker {ticker}.")
        try:
            with open(dov_path, "r+", encoding="utf-8") as f:
                data = json.load(f)
                data["ticker_index_finished"]["Long"] = index - 1  # Save last index
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()
        except Exception as e:
            print(f"Error updating progress in division_of_labor.json: {e}")
    print("Completed crawling assigned companies' OHLC data.")
    return
            
def get_latest_ohlc(ticker=None):
    """Get the most recent OHLC record based on timestamp t"""
    filter_query = {}
    if ticker:
        filter_query["ticker"] = ticker
        
    result = mongodb._OHLC.find_one(
        filter=filter_query,
        sort=[("t", DESCENDING)]
    )
    return result

def get_earliest_ohlc(ticker=None):
    """Get the most recent OHLC record based on timestamp t"""
    filter_query = {}
    if ticker:
        filter_query["ticker"] = ticker
        
    result = mongodb._OHLC.find_one(
        filter=filter_query,
        sort=[("t", ASCENDING)]
    )
    return result

def load_ohlc_to_db(ticker, from_timestamp, to_timestamp, time_update):
    list_ohlc, next_url = get_ohlc(ticker, from_timestamp, to_timestamp)
    if not list_ohlc:
        print(f"No new OHLC data for ticker {ticker}")
        return
    load_all_ohlc_to_db(ticker, list_ohlc, time_update)
    print(f"Fetched until {timestamp_to_YYYYMMDDTHH(list_ohlc[-1].get('t') / 1000) if list_ohlc else 'N/A'}")
    time.sleep(12)
    
    while next_url:
        list_ohlc, next_url = ohlc_get_next_url(next_url)
        if not list_ohlc:
            print(f"No new OHLC data for ticker {ticker}")
            time.sleep(12)
            break
        load_all_ohlc_to_db(ticker, list_ohlc, time_update)
        print(f"Fetched until {timestamp_to_YYYYMMDDTHH(list_ohlc[-1].get('t') / 1000) if list_ohlc else 'N/A'}")
        time.sleep(12)