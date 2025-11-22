# ingest_logs.py
import sqlite3
import pandas as pd
from pathlib import Path

DB = Path('logs.db')
CSV = Path('sample_logs.csv')

def ingest():
    df = pd.read_csv(CSV, parse_dates=['timestamp'])
    conn = sqlite3.connect(DB)
    # write table; replace if exists for demo
    df.to_sql('events', conn, if_exists='replace', index=False,
              dtype={
                  'timestamp':'TEXT',
                  'ip':'TEXT',
                  'username':'TEXT',
                  'event':'TEXT',
                  'bytes':'INTEGER'
              })
    conn.close()
    print(f"Ingested {len(df)} rows into {DB}")

if __name__ == '__main__':
    ingest()
