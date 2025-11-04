import config
from pymongo import MongoClient

client = MongoClient(config.MONGODB_URL)
db = client["stock_big_data"]

company_infos_coll = db['company_infos']
market_status_coll = db['market_status']
news_sentiment_coll = db['news_sentiment']
OHLC_coll = db['ohlc']