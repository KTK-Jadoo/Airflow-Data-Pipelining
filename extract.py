# extract.py
import requests
import json
import os
from datetime import datetime

def fetch_data():
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': 'AAPL',
        'apikey': os.getenv
    }
    response = requests.get(url, params=params)
    data = response.json()
    filename = f'/path/to/hdfs/data/raw/{datetime.now().strftime("%Y-%m-%d")}_data.json'
    with open(filename, 'w') as f:
        json.dump(data, f)
