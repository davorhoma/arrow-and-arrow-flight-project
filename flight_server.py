import pyarrow as pa
import pyarrow.flight as flight
import adbc_driver_postgresql.dbapi as adbc_dbapi
from pymongo import MongoClient
import duckdb

common_schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp('us'))
])

class SensorFlightServer(flight.FlightServerBase):
    def do_get(self, context, ticket):
        dataset_name = ticket.ticket.decode('utf-8')
        
        if dataset_name == 'postgres':
            table = self._get_postgres_data()
        elif dataset_name == 'mongo':
            table = self._get_mongo_data()
        elif dataset_name == 'duckdb':
            table = self._get_duckdb_data()
        elif dataset_name == 'postgres_values':
            table = self._get_postgres_values()
        elif dataset_name == 'mongo_values':
            table = self._get_mongo_values()
        elif dataset_name == 'duckdb_values':
            table = self._get_duckdb_values()
        elif dataset_name == 'postgres_json':
            table = self._get_postgres_json()
        elif dataset_name == 'mongo_json':
            table = self._get_mongo_json()
        elif dataset_name == 'duckdb_json':
            table = self._get_duckdb_json()
        else:
            raise ValueError("Nepoznat dataset")

        return flight.RecordBatchStream(table)

    def _get_postgres_data(self):
        uri = "postgresql://admin:admin@localhost:5432/postgresdb"
        with adbc_dbapi.connect(uri) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, value, timestamp FROM sensor_data")
                table = cur.fetch_arrow_table()
                return table.cast(common_schema)

    def _get_mongo_data(self):
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        db = client["test_db"]
        data = list(db["sensor_data"].find({}, {"_id": 0}))
        return pa.Table.from_pylist(data, schema=common_schema)
    
    def _get_duckdb_data(self):
        conn = duckdb.connect(database='sensor_data.duckdb', read_only=True)
        table = conn.execute("SELECT id, name, value, timestamp FROM sensor_data").arrow()
        return table.cast(common_schema)
    
    def _get_postgres_values(self):
        uri = "postgresql://admin:admin@localhost:5432/postgresdb"
        with adbc_dbapi.connect(uri) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM sensor_data")
                table = cur.fetch_arrow_table()
                return table

    def _get_mongo_values(self):
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        db = client["test_db"]
        data = list(db["sensor_data"].find({}, {"_id": 0, "value": 1}))
        return pa.Table.from_pylist(data)
    
    def _get_duckdb_values(self):
        conn = duckdb.connect(database='sensor_data.duckdb', read_only=True)
        table = conn.execute("SELECT value FROM sensor_data").arrow()
        return table
    
    def _table_to_json_arrow(self, table):
        import json
        json_string = json.dumps(table.to_pylist(), default=str)
        return pa.Table.from_pydict({'json_data': [json_string]})

    def _get_postgres_json(self):
        table = self._get_postgres_data()
        return self._table_to_json_arrow(table)

    def _get_mongo_json(self):
        table = self._get_mongo_data()
        return self._table_to_json_arrow(table)

    def _get_duckdb_json(self):
        table = self._get_duckdb_data().read_all()
        return self._table_to_json_arrow(table)

if __name__ == "__main__":
    location = "grpc+tcp://localhost:8888"
    server = SensorFlightServer(location)
    print(f"Flight Server sluša na {location}")
    server.serve()