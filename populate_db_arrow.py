import numpy as np
import pyarrow as pa
import duckdb
import psycopg2
from psycopg2 import extras
from pymongo import MongoClient
import datetime

NUM_RECORDS = 1_000_000

MONGO_URI = "mongodb://admin:admin@localhost:27017/"
MONGO_DB = "test_db"
MONGO_COLLECTION = "sensor_data"

PG_CONN = "dbname=postgresdb user=admin password=admin host=localhost port=5432"
DUCKDB_PATH = "sensor_data.duckdb"

def generate_data_arrow(n):
    """Efikasno generisanje 1M redova koristeći NumPy i Arrow."""
    print(f"--- Generisanje {n:,} redova u RAM-u ---")
    
    ids = np.arange(n)
    sensor_names = [f"Sensor_{i}" for i in range(1, 51)]
    names = np.random.choice(sensor_names, size=n)
    values = np.random.uniform(10.5, 95.5, n).round(2)
    now = datetime.datetime.utcnow()
    timestamps = np.full(n, now, dtype='datetime64[us]')

    table = pa.table({
        "id": ids,
        "name": names,
        "value": values,
        "timestamp": timestamps
    })
    return table

def seed_mongodb(arrow_table):
    print("Povezivanje na MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    collection.delete_many({})
    
    print("Prebacivanje u Mongo (bulk insert)...")
    data_dict = arrow_table.to_pylist()
    collection.insert_many(data_dict)
    
    print(f"Uspešno ubačeno u MongoDB.")
    client.close()

def seed_postgres(arrow_table):
    print("Povezivanje na Postgres...")
    conn = psycopg2.connect(PG_CONN)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS sensor_data;")
    cur.execute("""
        CREATE TABLE sensor_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            value FLOAT,
            timestamp TIMESTAMP
        );
    """)
    
    print("Prebacivanje u Postgres (konverzija tipova i bulk insert)...")
    
    data_tuples = list(zip(
        arrow_table['id'].to_pylist(),
        arrow_table['name'].to_pylist(),
        arrow_table['value'].to_pylist(),
        arrow_table['timestamp'].to_pylist()
    ))
    
    insert_query = "INSERT INTO sensor_data (id, name, value, timestamp) VALUES %s"
    extras.execute_values(cur, insert_query, data_tuples)
    
    conn.commit()
    print(f"Uspešno ubačeno u Postgres.")
    cur.close()
    conn.close()

def seed_duckdb(arrow_table):
    print("Povezivanje na DuckDB...")
    conn = duckdb.connect(database=DUCKDB_PATH)
    
    conn.execute("DROP TABLE IF EXISTS sensor_data;")
    
    print("Prebacivanje u DuckDB (Arrow Zero-copy)...")
    conn.execute("CREATE TABLE sensor_data AS SELECT * FROM arrow_table")
    
    print(f"Uspešno ubačeno u DuckDB.")
    conn.close()

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    try:
        data_table = generate_data_arrow(NUM_RECORDS)
        
        seed_duckdb(data_table)
        seed_postgres(data_table)
        seed_mongodb(data_table)
        
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"\nSve baze su uspešno popunjene za {duration:.2f} sekundi!")
        
    except Exception as e:
        print(f"\nKritična greška: {e}")