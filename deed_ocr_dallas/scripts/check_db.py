import sqlite3
import pandas as pd

def check_latest_entries(db_path='deed_data.db', num_entries=5):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM deeds ORDER BY rowid DESC LIMIT {num_entries}"
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(df)

if __name__ == "__main__":
    check_latest_entries()

