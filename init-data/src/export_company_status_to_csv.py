import json, csv
from config import (mic_to_idx,
                    GlobalConfig,
                    _init_env)

def export_data_of_companies_table() -> None:
    header = ["company_id",
              "company_cik",
              "company_composite_figi",
              "company_market_locale",
              "company_share_class_figi",
              "company_asset_type",
              "company_ticker",
              "company_name"]
    
    with open(GlobalConfig.COMPANY_INFOS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)

        with open(GlobalConfig.COMPANIES_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)
            
            for idx, item in enumerate(data, start=1):
                item = dict(item)

                company_id = idx
                company_cik = item.get("cik")
                company_composite_figi = item.get("composite_figi")
                company_market_locale = item.get("locale")
                company_share_class_figi = item.get("share_class_figi")
                company_asset_type = item.get("market")
                company_ticker = item.get("ticker")
                company_name = item.get("name")

                row = [company_id,
                       company_cik,
                       company_composite_figi,
                       company_market_locale,
                       company_share_class_figi,
                       company_asset_type,
                       company_ticker,
                       company_name]
                
                csv_writer.writerow(row)

def export_data_of_company_status_table() -> None:
    header = ["company_status_time_update",
              "company_status_type",
              "company_status_active",
              "company_status_fk",
              "company_status_primary_exchange"]
    
    with open(GlobalConfig.COMPANY_INFOS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)

        with open(GlobalConfig.COMPANY_STATUS_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            for idx, item in enumerate(data, start=1):
                item = dict(item)
                company_status_time_update = item.get("time_update")
                company_status_type = item.get("type")
                company_status_active = item.get("active")
                company_status_fk = idx
                company_status_primary_exchange = mic_to_idx[item.get("primary_exchange")] if item.get("primary_exchange") != None else None

                row = [
                    company_status_time_update,
                    company_status_type,
                    company_status_active,
                    company_status_fk,
                    company_status_primary_exchange,
                ]

                csv_writer.writerow(row)

_init_env()
export_data_of_companies_table()
export_data_of_company_status_table()