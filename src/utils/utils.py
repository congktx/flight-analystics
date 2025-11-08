from typing import List, Dict, Tuple
from utils.config import *
import json
from pprint import pprint
import os
from datetime import datetime, timezone

def get_company_labels(task_owner: str) -> Tuple[List[str], int]:
    with open(file=GlobalConfig.LABELS_PATH, mode='r') as file:
        labels_dict = dict(json.load(fp=file))
        labels_list = labels_dict[task_owner]
    
    with open(file=GlobalConfig.INDEX_PATH, mode="r") as file:
        finished_index = int(file.readline())

    return labels_list[finished_index:], finished_index

def save_finished_index(index: int) -> None:

    with open(file=GlobalConfig.INDEX_PATH, mode="w") as file:
        file.write(str(index))

def save_info(path: str, data: List[Dict], id: int):
    print(f"------------------{id}")
    file_path = f"{path}/{id}.json"
    print(file_path)

    with open(file_path, mode="w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def parse_date_to_timestamp(date: str):
    dt = datetime.strptime(date, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    timestamp = dt.timestamp()
    return timestamp

def timestamp_to_YYYYMMDDTHHMM(timestamp):
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y%m%dT%H%M")