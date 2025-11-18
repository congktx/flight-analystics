import threading
from tqdm import tqdm

from service import *

if __name__ == '__main__':
    for year in [2025]:
        for month in range(4,13):     
            while True:
                try:
                    month_str = str(month)
                    if month < 10: 
                        month_str = '0' + month_str

                    print(f"{year}-{month_str}-01")

                    crawl_all_company(date=f"{year}-{month_str}-01", list_exchage=['XNAS', 'XNYS'])
                    # crawl_news_sentiment(1704067200, 1761969690, 1704067200)
                    # crawl_all_ohlc(1704067200, 1761969690, 1704067200)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(10)
    print("Finish")