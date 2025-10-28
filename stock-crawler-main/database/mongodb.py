import sys
import logging

from pymongo import MongoClient
from dotenv import load_dotenv
from config import MongoDBConfig
from utils.time_utils import round_timestamp

load_dotenv()

logger = logging.getLogger("mongodb")


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        try:
            self.client = MongoClient(connection_url)
            self.db = self.client[MongoDBConfig.DATABASE]
        except Exception:
            logger.warning("Failed connecting to MongoDB Main")
            sys.exit()

        self._company_infos = self.get_collection('company_infos')
        self._market_status = self.get_collection('market_status')
        self._news_sentiment = self.get_collection('news_sentiment')
        self._OHLC = self.get_collection('OHLC')

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def upsert_space_company(self, company_infos):
        result = self._company_infos.update_one(
            {"_id": company_infos["_id"]},
            {"$setOnInsert": company_infos},
            upsert=True
        )
        return result.upserted_id
    
    def upsert_space_market(self, market_infos):
        result = self._market_status.update_one(
            {"_id": market_infos["_id"]},
            {"$setOnInsert": market_infos},
            upsert=True
        )
        return result.upserted_id
