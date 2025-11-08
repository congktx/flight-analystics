import os

ROOT_PATH=None
class GlobalConfig:
    LABELS_PATH='label/division_of_labor.json'
    INDEX_PATH='label/finished_index.txt'
    COMPANY_DATA_PATH='data/company'
    MARKET_DATA_PATH='data/market'
    NEWS_DATA_PATH='data/news'
    OHLC_DATA_PATH='data/ohlc'

class MassiveConfig:
    API_KEY="H74eWNrlTbSfr4N0VPagBGhM5f2vEIYn"

class AlphavantageConfig:
    API_KEY="T4NKJJKRQJQHQG0Z"
    URL="https://www.alphavantage.co/query"

def _init_env():
    global ROOT_PATH
    cur_dir = os.getcwd()
    ROOT_PATH = os.path.abspath(os.path.join(cur_dir, ".."))
    __init_global_config()

def __init_global_config():
    GlobalConfig.LABELS_PATH=os.path.join(ROOT_PATH, GlobalConfig.LABELS_PATH)
    GlobalConfig.INDEX_PATH=os.path.join(ROOT_PATH, GlobalConfig.INDEX_PATH)
    GlobalConfig.COMPANY_DATA_PATH=os.path.join(ROOT_PATH, GlobalConfig.COMPANY_DATA_PATH)
    GlobalConfig.MARKET_DATA_PATH=os.path.join(ROOT_PATH, GlobalConfig.MARKET_DATA_PATH)
    GlobalConfig.NEWS_DATA_PATH=os.path.join(ROOT_PATH, GlobalConfig.NEWS_DATA_PATH)
    GlobalConfig.OHLC_DATA_PATH=os.path.join(ROOT_PATH, GlobalConfig.OHLC_DATA_PATH)