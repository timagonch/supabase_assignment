# supabase_pull.py
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import pandas as pd

load_dotenv()  # reads .env

url: str = os.getenv("SUPABASE_URL")          
key: str = os.getenv("SUPABASE_ANON_KEY")     

supabase: Client = create_client(url, key)

def fetch_table(table_name: str, limit: int = 50):
    # simple pull (first N rows)
    resp = supabase.table(table_name).select("*").limit(limit).execute()
    return resp.data

def fetch_all(table_name: str, page_size: int = 1000):
    # pulls all rows in pages using .range()
    offset = 0
    all_rows = []
    while True:
        resp = (
            supabase.table(table_name)
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows

if __name__ == "__main__":
    table = "battles"

    # Option A: sample
    rows = fetch_table(table, limit=50)
    # Option B: everything
    # rows = fetch_all(table, page_size=1000)

    if rows:
        df = pd.DataFrame(rows)
        print(df.head())

    else:
        print("No data returned.")
