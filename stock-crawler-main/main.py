import threading
from tqdm import tqdm

from service import *

if __name__ == '__main__':
    # crawl_all_company(list_exchage=['XNYS'])
    # crawl_news_sentiment(1733011200, 1746057600, 1746057600)

    # crawl_all_ohlc(1704067200, 1759190400, 1759190400)
    
    crawl_assigned_companies_ohlc(1733011200, 1759190400, 1759190400)
