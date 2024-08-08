# load.py
import pandas as pd
from sqlalchemy import create_engine

def load_data():
    processed_data_path = '/path/to/hdfs/data/processed/*.json'
    df = pd.read_json(processed_data_path)
    engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
    df.to_sql('financial_data', engine, if_exists='replace')

if __name__ == "__main__":
    load_data()
