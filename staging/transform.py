"""
Pipeline Staging
- Agrège les données minute en daily depuis Elasticsearch
- Calcule les indicateurs techniques sur données daily
- Stocke dans PostgreSQL
"""

import pandas as pd
import numpy as np
import psycopg2
from elasticsearch import Elasticsearch
from datetime import datetime

es = Elasticsearch("http://localhost:9200")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="crypto_lake",
    user="nathan",
    password="password"
)


def create_staging_table():
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS staging_btc")
        cur.execute("""
            CREATE TABLE staging_btc (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP UNIQUE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                rsi FLOAT,
                bb_upper FLOAT,
                bb_lower FLOAT,
                bb_middle FLOAT,
                volatility_7d FLOAT,
                volatility_30d FLOAT,
                returns FLOAT,
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
    print("Table staging_btc recréée")


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def compute_bollinger_bands(series: pd.Series, period: int = 20):
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    return middle + (2 * std), middle, middle - (2 * std)


def fetch_and_aggregate() -> pd.DataFrame:
    """
    Récupère toutes les données depuis ES via scroll
    et les agrège en daily OHLCV.
    """
    print("Récupération de toutes les données (scroll)...")

    all_records = []

    result = es.search(
        index="raw_btc_historical",
        scroll="5m",
        size=5000,
        body={
            "query": {"match_all": {}},
            "sort": [{"timestamp": {"order": "asc"}}],
            "_source": ["timestamp", "open", "high", "low", "close", "volume"]
        }
    )

    scroll_id = result["_scroll_id"]
    hits = result["hits"]["hits"]
    all_records.extend([h["_source"] for h in hits])
    print(f"Batch 1 : {len(all_records)} documents...")

    batch = 2
    while len(hits) > 0:
        result = es.scroll(scroll_id=scroll_id, scroll="5m")
        scroll_id = result["_scroll_id"]
        hits = result["hits"]["hits"]
        all_records.extend([h["_source"] for h in hits])
        if batch % 10 == 0:
            print(f"Batch {batch} : {len(all_records)} documents...")
        batch += 1

    es.clear_scroll(scroll_id=scroll_id)
    print(f"{len(all_records)} documents récupérés au total")

    df = pd.DataFrame(all_records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date

    print("Agrégation en daily OHLCV...")
    daily = df.groupby("date").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum")
    ).reset_index()

    daily["timestamp"] = pd.to_datetime(daily["date"])
    daily = daily.drop(columns=["date"])
    daily = daily.sort_values("timestamp").reset_index(drop=True)

    print(f"{len(daily)} jours de données ({daily['timestamp'].min().date()} → {daily['timestamp'].max().date()})")
    return daily


def transform_and_load(df: pd.DataFrame):
    print("Calcul des indicateurs techniques...")

    df["returns"] = df["close"].pct_change()
    df["rsi"] = compute_rsi(df["close"])
    df["bb_upper"], df["bb_middle"], df["bb_lower"] = compute_bollinger_bands(df["close"])
    df["volatility_7d"] = df["returns"].rolling(window=7).std() * np.sqrt(7)
    df["volatility_30d"] = df["returns"].rolling(window=30).std() * np.sqrt(30)

    df = df.dropna()
    print(f"Features calculées sur {len(df)} jours")

    print("Chargement dans PostgreSQL...")
    with conn.cursor() as cur:
        inserted = 0
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO staging_btc
                        (timestamp, open, high, low, close, volume,
                         rsi, bb_upper, bb_lower, bb_middle,
                         volatility_7d, volatility_30d, returns)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (timestamp) DO NOTHING
                """, (
                    row["timestamp"], row["open"], row["high"],
                    row["low"], row["close"], row["volume"],
                    row["rsi"], row["bb_upper"], row["bb_lower"],
                    row["bb_middle"], row["volatility_7d"],
                    row["volatility_30d"], row["returns"]
                ))
                inserted += 1
            except Exception as e:
                conn.rollback()
                continue
        conn.commit()
    print(f"{inserted} jours chargés dans staging_btc")


if __name__ == "__main__":
    print("Démarrage pipeline Staging (version daily)...")
    create_staging_table()
    df = fetch_and_aggregate()
    transform_and_load(df)
    print("Pipeline Staging terminé !")