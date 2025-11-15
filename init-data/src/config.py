import os
mic_code = {
    "NASDAQ": "XNAS",
    "NYSE": "XNYS",
    "AMEX": "XASE",
    "BATS": "BATS",
    "Toronto": "XTSE",
    "Toronto Ventures": "XTSX",
    "London": "XLON",
    "XETRA": "XETR",
    "Berlin": "XBER",
    "Frankfurt": "XFRA",
    "Munich": "XMUN",
    "Stuttgart": "XSTU",
    "Paris": "XPAR",
    "Barcelona": "XBAR",
    "Madrid": "XMAD",
    "Lisbon": "XLIS",
    "Tokyo": "XTKS",
    "NSE": "XNSE",
    "BSE": "XBOM",
    "Shanghai": "XSHG",
    "Shenzhen": "XSHE",
    "Hong Kong": "XHKG",
    "Sao Paolo": "BVMF",   
    "Mexico": "XMEX",
    "Johannesburg": "XJSE",
    "Global": "FOREX"      
}

mic_to_idx = {
    "XNAS": 1,
    "XNYS": 2
}

class GlobalConfig:
    MARKET_STATUS_PATH = "data/json/stock-analystics.market_status.json"
    COMPANY_INFOS_PATH = "data/json/stock-analystics.company_infos.json"
    EXCHANGES_TABLE_PATH = "data/csv/exchanges.csv"
    COMPANIES_TABLE_PATH = "data/csv/companies.csv"
    MARKETS_TABLE_PATH = "data/csv/markets.csv"
    MARKET_STATUS_TABLE_PATH = "data/csv/market_status.csv"
    COMPANY_STATUS_TABLE_PATH = "data/csv/company_status.csv"
    ROOT = None 

def _init_env():
    GlobalConfig.ROOT = os.path.join(os.getcwd(), "..")
    GlobalConfig.MARKET_STATUS_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKET_STATUS_PATH)
    GlobalConfig.COMPANY_INFOS_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANY_INFOS_PATH)
    GlobalConfig.EXCHANGES_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.EXCHANGES_TABLE_PATH)
    GlobalConfig.COMPANIES_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANIES_TABLE_PATH)
    GlobalConfig.MARKETS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKETS_TABLE_PATH)
    GlobalConfig.MARKET_STATUS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKET_STATUS_TABLE_PATH)
    GlobalConfig.COMPANY_STATUS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANY_STATUS_TABLE_PATH)

