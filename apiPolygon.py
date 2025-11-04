import requests
import mongoConnection as mongo
import json
import time

with open('split_ticker.json', 'r') as file:
  companies = json.load(file)

cnt = 0
for ticker in companies["Cong"]:
  try:
    company_find = mongo.company_infos_coll.find_one({"ticker": ticker})
    if (company_find and company_find.get('isQueryForOHLCByBTC')): continue
    print(ticker)

    if (cnt): time.sleep(12)
    time_from = '2024-01-01'
    time_to = '2025-11-03'
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/hour/{time_from}/{time_to}/?adjusted=true&sort=asc&apiKey=2tfMqXwD_TM9nx0EunKdcQWiywLJfHDX'
    r = requests.get(url)
    data = r.json()
    if (not data.get('results')): continue

    results = data['results']
    for ohlc in results:
      try:
        document = {
           "_id": ticker + '_' + str(ohlc['t']),
            "ticker": ticker,
            "t": ohlc['t'],
            "o": ohlc['o'],
            "h": ohlc['h'],
            'l': ohlc['l'],
            'c': ohlc['c'],
            'v': ohlc['v'],
        }
        mongo.OHLC_coll.find_one_and_update({"_id": document['_id']}, {"$set": document}, upsert=True)
      except Exception as e:
        print(e)

    cnt += 1
    print(cnt)

    mongo.company_infos_coll.find_one_and_update({"_id": company_find['_id']}, {"$set":{'isQueryForOHLCByBTC': True}})
  except Exception as e:
    print(e)
