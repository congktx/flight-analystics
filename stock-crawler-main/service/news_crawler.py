import requests

from config import AlphavantageConfig

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
    timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    filter = {
        "time_update": timestamp
    }
    list_company_infos = list(mongodb.find_documets(mongodb._company_infos, filter))
    tickers = list(map(lambda x: x.get('ticker'), list_company_infos))

    for ticker in tickers:
        list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
        print(ticker, len(list_news))
        if list_news == "rate limit": 
            print(f"rate limit at {ticker}")
            break
        load_all_news_sentiment_to_db(list_news, time_update)
        time.sleep(5)
        
def crawl_assigned_news_sentiment(from_timestamp, to_timestamp, time_update):
    # load division_of_labor.json from project root
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dov_path = os.path.join(root, "division_of_labor.json")
    
    try:
        with open(dov_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            tickers = data.get(AssignedCompaniesConfig.ASSIGNED_COMPANIES, [])
            last_index = data.get("ticker_index_finished", {}).get("Long", {}).get("sentiments", -1)
            print("tickers counted:", len(tickers))
    except Exception as e:
        print(f"Error loading division_of_labor.json: {e}")
        return
        last_index = -1
        tickers = []
    
    for index in range(last_index + 1, len(tickers)):
        ticker = tickers[index]
        list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
        print(ticker, len(list_news))
        if list_news == "rate limit": 
            print(f"rate limit at {ticker}")
            # break
            return "rate limit"
        load_all_news_sentiment_to_db(list_news, time_update)
        
        try:
            with open(dov_path, "r+", encoding="utf-8") as f:
                data = json.load(f)
                data["ticker_index_finished"]["Long"]["sentiments"] = index  # Save last index
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()
        except Exception as e:
            print(f"Error updating progress in division_of_labor.json: {e}")
            
        time.sleep(1)