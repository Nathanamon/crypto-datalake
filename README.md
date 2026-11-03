# Crypto Data Lake

End-to-end data pipeline for Bitcoin market analysis.
Ingestion, transformation, market regime clustering and exposure via API.

## Architecture
Raw (Elasticsearch) → Staging (PostgreSQL) → Curated (PostgreSQL) → API (FastAPI)

| Zone | Technology | Content |
|---|---|---|
| Raw | Elasticsearch | 7.6M minute-level OHLCV entries + live CoinGecko prices |
| Staging | PostgreSQL | Daily data + technical indicators (RSI, Bollinger, volatility) |
| Curated | PostgreSQL | Market regimes classified (bull/bear/sideways) via KMeans |

## Data sources

- **File**: [Bitcoin Historical Data](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data) (Kaggle) — minute-level OHLCV data since 2012
- **API**: [CoinGecko API](https://www.coingecko.com/en/api) — live price, market cap, volume (free, no key required)

## Tech stack

- **Elasticsearch 8.11** — raw zone storage
- **PostgreSQL 15** — staging and curated zone storage
- **Apache Airflow 2.8** — orchestration and scheduling
- **FastAPI** — API Gateway
- **Scikit-learn** — KMeans clustering (market regime detection)
- **Docker / Docker Compose** — containerization

## Installation and setup

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+
- Kaggle account (to download the dataset)

### 1. Clone the repo

```bash
git clone <repo-url>
cd crypto-datalake
```

### 2. Download the Kaggle dataset

Download the CSV file from:
https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data

Place the file in `data/raw/btc_historical.csv`

### 3. Start the services

```bash
docker-compose up -d
```

Wait 30-60 seconds for Elasticsearch and PostgreSQL to start.

### 4. Install the Python dependencies

```bash
pip install -r requirements.txt
```

### 5. Run the full pipeline

```bash
# Raw zone: CSV ingestion + CoinGecko API
python ingestion/ingest_raw.py

# Staging zone: daily aggregation + technical indicators
python staging/transform.py

# Curated zone: KMeans clustering
python curated/clustering.py
```

### 6. Start the API

```bash
cd api
uvicorn main:app --reload --port 8000
```

## API Gateway

Base URL: `http://localhost:8000`

| Endpoint | Description |
|---|---|
| `GET /health` | Status of all services |
| `GET /stats` | Fill metrics per zone |
| `GET /raw?limit=10` | Latest raw entries (Elasticsearch) |
| `GET /raw/live` | Latest live CoinGecko price |
| `GET /staging?limit=10` | Data with technical indicators |
| `GET /curated?limit=10` | Data with market regimes |
| `GET /curated/current-regime` | Current market regime |

Interactive documentation: `http://localhost:8000/docs`

## Airflow orchestration

Interface: `http://localhost:8080` (admin / admin)

| DAG | Schedule | Description |
|---|---|---|
| `ingest_live_dag` | @hourly | Ingests the live BTC price from CoinGecko |
| `full_pipeline_dag` | @daily | Recomputes staging + curated |

The `ingest_live_dag` DAG uses **XCom** to pass the BTC price between the `ingest_coingecko` and `log_price` tasks.

## ML — Market regime detection

The curated pipeline applies **KMeans clustering (k=3)** on the following features:
- RSI (Relative Strength Index)
- 7-day rolling volatility
- 30-day rolling volatility
- Daily returns

The data is normalized with `StandardScaler` before clustering.
The 3 clusters are automatically labeled **bull / bear / sideways**
based on the mean returns of each cluster.

### Results on 2012-2026 data

| Regime | Periods | Mean return | 7-day volatility |
|---|---|---|---|
| Bull | 2093 days | +0.014 | 0.069 |
| Bear | 2594 days | -0.009 | 0.069 |
| Sideways | 590 days | +0.011 | 0.203 |


## Technical choices

**Why Elasticsearch for the Raw zone?**
Elasticsearch is optimized for indexing and searching large volumes of unstructured JSON documents. With 7.6M minute-level data points, it allows fast batch ingestion and flexible queries on the raw data.

**Why PostgreSQL for Staging and Curated?**
The staging and curated zones contain structured, aggregated (daily) data on which we run analytical SQL queries. PostgreSQL is the natural choice for this kind of relational data.

**Why KMeans for regime detection?**
KMeans is an accessible and interpretable unsupervised clustering algorithm. With k=3 clusters and prior feature normalization, it can naturally distinguish market phases without needing labeled data.
