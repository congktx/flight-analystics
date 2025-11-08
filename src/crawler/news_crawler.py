from utils.utils import (parse_date_to_timestamp, 
                         timestamp_to_YYYYMMDDTHHMM,
                         save_info,
                         get_company_labels,
                         save_finished_index)
from pprint import pprint
import requests
from utils.config import *

def get_news_sentiment_by_stickers(tickers: str, from_date: str, to_date: str, id):

    time_from = timestamp_to_YYYYMMDDTHHMM(parse_date_to_timestamp(date=from_date))
    time_to = timestamp_to_YYYYMMDDTHHMM(parse_date_to_timestamp(date=to_date))
    url = AlphavantageConfig.URL

    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": tickers,
        "apikey": AlphavantageConfig.API_KEY,
        "limit": 1000,
        "time_from": time_from,
        "time_to": time_to
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, params=params, headers=headers).json()
        if response.get('Information') and "rate limit" in response.get('Information'):
            print("===================== Daily rate limit reached please change your IP =================")
            return None
        if response.get('feed'):
            list_news = []
            for new in response.get('feed'):
                new.update({
                    "sentiment_score_definition": response.get('sentiment_score_definition'),
                    "relevance_score_definition": response.get('relevance_score_definition')
                })
                list_news.append(new)
            save_info(path=GlobalConfig.NEWS_DATA_PATH,
                      data=list_news,
                      id=id)
            save_finished_index(index=id)
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    
def get_news_sentiment(from_date: str, to_date: str):
    tickers, id = get_company_labels(task_owner="Loi")
    for idx, ticker in enumerate(tickers):
        if get_news_sentiment_by_stickers(tickers=ticker, 
                                       from_date=from_date, 
                                       to_date=to_date,
                                       id=idx+id+1) == None:
            return None
