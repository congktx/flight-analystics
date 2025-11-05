import pymongo
from datetime import datetime

# MongoDB connection details
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['test']
collection = db['OHLC']

ticker = input('Enter the ticker symbol: ')

# Query to find records with the specified ticker
records = collection.find({'ticker': ticker})

# Extract and print human-readable dates
for record in records:
    timestamp = record.get('t')
    if isinstance(timestamp, int):  # Ensure 't' is of type long
        human_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        print(f'Ticker: {ticker}, Date: {human_date}')