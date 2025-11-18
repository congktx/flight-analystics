import os
from dotenv import load_dotenv
load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL")
SPEC_API_TOKEN = os.getenv("SPEC_API_TOKEN")
ALPHAVANTAGE_TOKEN=os.getenv("ALPHAVANTAGE_TOKEN")
POLYGONIO_TOKEN=os.getenv("POLYGONIO_TOKEN")
DATA_JSON_PATH=os.getenv("DATA_JSON_PATH")