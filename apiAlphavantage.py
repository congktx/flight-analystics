import requests
import config
import mongoConnection as mongo
import json

with open('split_ticker.json', 'r') as file:
  companies = json.load(file)

# url = f'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey=${config.ALPHAVANTAGE_TOKEN}'
# r = requests.get(url)
# data = r.json()

# print(data)

# new sentiment
cnt = 0
for ticker in companies["Cong"]:
  try:
    company_find = mongo.company_infos_coll.find_one({"ticker": ticker})
    if (company_find and company_find.get('isQueryForNewSentimentByBTC')): continue

    time_from = '20240101T0000'
    time_to = '20251103T0000'
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={config.ALPHAVANTAGE_TOKEN}&time_from={time_from}&time_to={time_to}'
    r = requests.get(url)
    data = r.json()
    if (not data.get('feed')): continue

    feed = data['feed']
    for fee in feed:
      try:
        mongo.news_sentiment_coll.find_one_and_update({"_id": fee['title']}, {"$set": fee}, upsert=True)
      except Exception as e:
        print(e)

    cnt += 1
    print(cnt)

    mongo.company_infos_coll.find_one_and_update({"_id": company_find['_id']}, {"$set":{'isQueryForNewSentimentByBTC': True}})
  except Exception as e:
    print(e)

