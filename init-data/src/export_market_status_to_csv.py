import json, csv
from pprint import pprint
from config import (mic_code, 
                    GlobalConfig,
                    _init_env)

def export_data_of_exchanges_table() -> None:
    header = ["exchange_id", 
              "exchange_mic", 
              "exchange_name", 
              "exchange_fk"]

    with open(GlobalConfig.MARKET_STATUS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)
        with open(GlobalConfig.EXCHANGES_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            exchange_id = 1
            for idx, item in enumerate(data, start=1):
                primary_exchanges = list(map(str.strip ,str(item["primary_exchanges"]).split(",")))
                for exchange in primary_exchanges:
                    exchange_mic = mic_code.get(exchange)
                    exchange_name = exchange
                    exchange_fk = idx

                    row = [exchange_id, exchange_mic, exchange_name, exchange_fk]
                    csv_writer.writerow(row)
                    exchange_id += 1

def export_data_of_markets_table() -> None:
    header = ["market_id", 
              "market_type", 
              "market_region", 
              "market_local_close", 
              "market_local_open"]
    
    with open(GlobalConfig.MARKET_STATUS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)

        with open(GlobalConfig.MARKETS_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            for idx, item in enumerate(data, start=1):
                item = dict(item)
                market_id = idx
                market_type = item.get("market_type")
                market_region = item.get("region")
                market_local_close = item.get("local_close")
                market_local_open = item.get("local_open")

                row = [market_id, 
                       market_type, 
                       market_region, 
                       market_local_close, 
                       market_local_open]
                
                csv_writer.writerow(row)

def export_data_of_market_status_table() -> None:
    header = ["market_status_time_update",
              "market_status_current_status",
              "market_status_fk"]
    
    with open(GlobalConfig.MARKET_STATUS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)

        with open(GlobalConfig.MARKET_STATUS_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            for idx, item in enumerate(data, start=1):
                item = dict(item)

                market_status_time_update = item.get("time_update")
                market_status_current_status = item.get("current_status")
                market_status_fk = idx
                
                row = [market_status_time_update,
                       market_status_current_status,
                       market_status_fk]
                
                csv_writer.writerow(row)