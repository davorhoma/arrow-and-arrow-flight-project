import pyarrow.flight as flight
import pyarrow as pa
import pyarrow.compute as pc

import time

def get_only_values(client):
    start = time.time()
    postgres_ticket = flight.Ticket("postgres_values")
    reader = client.do_get(postgres_ticket)
    postgres_table = reader.read_all()
    end = time.time()
    print(f"\nVreme dobijanja values podataka iz Postgresa: {end - start:.5f} sekundi")

    start = time.time()
    mongo_ticket = flight.Ticket("mongo_values")
    reader = client.do_get(mongo_ticket)
    mongo_table = reader.read_all()
    end = time.time()
    print(f"Vreme dobijanja values podataka iz MongoDB-a: {end - start:.5f} sekundi")

    start = time.time()
    duckdb_ticket = flight.Ticket("duckdb_values")
    reader = client.do_get(duckdb_ticket)
    duckdb_table = reader.read_all()
    end = time.time()
    print(f"Vreme dobijanja values podataka iz DuckDB-a: {end - start:.5f} sekundi")

    return postgres_table, mongo_table, duckdb_table

client = flight.connect("grpc+tcp://localhost:8888")

start = time.time()
postgres_ticket = flight.Ticket("postgres")
reader = client.do_get(postgres_ticket)
postgres_table = reader.read_all()
end = time.time()
print(f"Vreme dobijanja podataka iz Postgresa: {end - start:.5f} sekundi")

start = time.time()
mongo_ticket = flight.Ticket("mongo")
reader = client.do_get(mongo_ticket)
mongo_table = reader.read_all()
end = time.time()
print(f"Vreme dobijanja podataka iz MongoDB-a: {end - start:.5f} sekundi")

start = time.time()
duckdb_ticket = flight.Ticket("duckdb")
reader = client.do_get(duckdb_ticket)
duckdb_table = reader.read_all()
end = time.time()
print(f"Vreme dobijanja podataka iz DuckDB-a: {end - start:.5f} sekundi")

postgres_values, mongo_values, duckdb_values = get_only_values(client)

mean_val_postgres = pc.mean(postgres_table["value"])
mean_val_mongo = pc.mean(mongo_table["value"])
mean_val_duckdb = pc.mean(duckdb_table["value"])
print(f"\nPostgres redova: {postgres_table.num_rows}, Prosek: {mean_val_postgres.as_py():.2f}")
print(f"MongoDB redova: {mongo_table.num_rows}, Prosek: {mean_val_mongo.as_py():.2f}")
print(f"DuckDB redova: {duckdb_table.num_rows}, Prosek: {mean_val_duckdb.as_py():.2f}")

print(f"\nPostgres values redova: {postgres_values.num_rows}, Prosek: {pc.mean(postgres_values['value']).as_py():.2f}")
print(f"MongoDB values redova: {mongo_values.num_rows}, Prosek: {pc.mean(mongo_values['value']).as_py():.2f}")
print(f"DuckDB values redova: {duckdb_values.num_rows}, Prosek: {pc.mean(duckdb_values['value']).as_py():.2f}")

# Spajanje (Concatenate) pošto imaju istu šemu
combined_table = pa.concat_tables([postgres_table, mongo_table, duckdb_table])
mean_val = pc.mean(combined_table["value"])
print(f"\nUkupno spojenih redova: {combined_table.num_rows}")
print(f"Prosečna vrednost svih senzora: {mean_val.as_py():.2f}")

# Merenje Arrow Compute (C++ implementacija, Arrow native)
start = time.time()
mean_arrow = pc.mean(combined_table["value"])
arrow_time = time.time() - start

# Merenje Pandas (C implementacija, ali zahteva konverziju)
start = time.time()
df = combined_table.to_pandas()
mean_pandas = df["value"].mean()
pandas_time = time.time() - start

print(f"\nArrow Compute vreme: {arrow_time:.5f} s")
print(f"Pandas (sa konverzijom) vreme: {pandas_time:.5f} s")
print(f"Arrow je {pandas_time / arrow_time:.1f}x brži u ovom scenariju.")


def benchmark_json(client, ticket_name):
    import json

    print(f"\n--- Benchmark: JSON vs Arrow za {ticket_name} ---")
    
    start = time.time()
    reader = client.do_get(flight.Ticket(ticket_name))
    arrow_table = reader.read_all()
    arrow_time = time.time() - start
    print(f"Arrow Flight vreme: {arrow_time:.4f} s")

    start = time.time()
    reader = client.do_get(flight.Ticket(f"{ticket_name}_json"))
    json_table = reader.read_all()
    # Deserijalizacija (najsporiji deo)
    raw_json = json_table.column(0)[0].as_py()
    data = json.loads(raw_json) 
    json_time = time.time() - start
    
    print(f"JSON (Transfer + Parsing) vreme: {json_time:.4f} s")
    print(f"Arrow je {json_time / arrow_time:.1f}x brži od JSON-a!")
    print(len(data))

benchmark_json(client, "postgres")
benchmark_json(client, "mongo")
benchmark_json(client, "duckdb")