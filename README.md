# Apache Arrow Flight – Multi-Database Performance Testing

Projekat za testiranje prenosa i obrade podataka pomoću **Apache Arrow Flight** radnog okvira. Demonstrira efikasan prenos velikih količina podataka između različitih baza podataka (PostgreSQL, MongoDB, DuckDB) koristeći Arrow format, sa klijentima implementiranim u **Python** i **Go** programskim jezicima.

## Pregled

Ovaj projekat implementira:

-   **Arrow Flight Server** (Python) – centralizovani server za dobavljanje podataka iz tri različite baze
-   **Python klijent** – analitika i benchmark testovi
-   **Go klijent** – demonstracija jezičke nezavisnosti Arrow formata
-   **Automatsko punjenje baza** sa 1 milion podataka

### Apache Arrow prednosti
-   **Zero-copy prenos** – podaci se ne kopiraju u memoriji
-   **Nezavisnost od jezika** – isti format u Python, Go, Rust, Java...
-   **Kolonski format** – optimizovan za analitičke upite
-   **gRPC transport** – brz i efikasan prenos podataka
-   **Native integracija** – DuckDB direktno podržava Arrow bez konverzije

### Poređenje performansi
Projekat uključuje benchmark testove koji pokazuju:
-   **Arrow vs JSON** – brzina prenosa i parsiranja
-   **Arrow vs Pandas** – brzina računanja (mean, agregacije)
-   **Zero-copy** – DuckDB direktno koristi Arrow tabele
-   **Multi-database** – objedinjavanje podataka iz različitih izvora

## Instalacija

### Preduslov

-   Docker & Docker Compose
-   Python 3.8+
-   Go 1.21+ (opciono, za Go klijent)

### 1. Pokretanje baza podataka

```bash
docker-compose up -d

```

Ovo podiže:

-   PostgreSQL na portu `5432`
-   MongoDB na portu `27017`

### 2. Instalacija Python zavisnosti

```bash
pip install -r requirements.txt

```

### 3. Punjenje baza sa testnim podacima

Generiše 1 milion podataka i popunjava sve tri baze:

```bash
python populate_db_arrow.py

```

**Očekivani rezultat:**

-   PostgreSQL: 1,000,000 redova
-   MongoDB: 1,000,000 dokumenata
-   DuckDB: 1,000,000 redova (file: `sensor_data.duckdb`)

## Upotreba

### 1. Pokretanje Arrow Flight servera

```bash
python flight_server.py

```

Server sluša na `localhost:8888` i izlaže sledeće endpoint-e (ticket-e):
| Ticket | Opis |
|--------|------|
| `postgres` | Svi podaci iz PostgreSQL |
| `mongo` | Svi podaci iz MongoDB |
| `duckdb` | Svi podaci iz DuckDB |
| `postgres_values` | Samo `value` kolona iz PostgreSQL |
| `mongo_values` | Samo `value` kolona iz MongoDB |
| `duckdb_values` | Samo `value` kolona iz DuckDB |
| `postgres_json` | PostgreSQL podaci kao JSON string |
| `mongo_json` | MongoDB podaci kao JSON string |
| `duckdb_json` | DuckDB podaci kao JSON string |

### 2. Python klijent

```bash
python flight_client.py

```

**Šta radi:**

1.  Dobavlja podatke iz sve tri baze preko Arrow Flight protokola
2.  Meri vreme prenosa za svaki dataset
3.  Računa prosečne vrednosti pomoću Arrow Compute API
4.  Spaja sve tabele i izvodi agregacije
5.  **Benchmark Arrow vs Pandas** – poredi brzinu računanja
6.  **Benchmark Arrow vs JSON** – poredi brzinu prenosa i deserijalizacije

### 3. Go klijent (opciono)

```bash
go mod init arrow-flight-client
go mod tidy
go run main.go

```

**Demonstracija jezičke nezavisnosti:**

Go klijent čita iste Arrow Flight stream-ove kao i Python klijent, bez potrebe za bilo kakvom konverzijom formata. Ovo potvrđuje da je Arrow format istinski nezavisan od programskog jezika.

## Tehnički detalji

### Šema podataka

Sve tri baze koriste jedinstvenu šemu:

```python
common_schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp('us'))
])

```

### Database-specific optimizacije

**PostgreSQL:**

-   Koristi `adbc_driver_postgresql` za direktnu Arrow integraciju
-   Zero-copy: podaci se direktno mapiraju u Arrow format

**MongoDB:**

-   Podaci se dobavljaju kao Python dict lista
-   Konverzija u Arrow tabelu preko `pa.Table.from_pylist()`

**DuckDB:**

-   **Najbrži prenos** jer je DuckDB nativno Arrow-kompatibilan
-   Zero-copy: rezultati upita se vraćaju kao Arrow tabele bez konverzije