from utils.config import _init_env, GlobalConfig
from crawler.company_crawler import get_company_infos
from crawler.market_crawler import get_market_status
from crawler.news_crawler import get_news_sentiment
from crawler.ohlc_crawler import get_ohlc_data
_init_env()

get_ohlc_data(from_date="2024-01-01",
              to_date="2024-12-30")
            