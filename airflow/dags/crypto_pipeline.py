"""
DAGs Airflow — Crypto Data Lake
DAG 1 : ingest_live_dag     → toutes les heures, ingère CoinGecko
DAG 2 : full_pipeline_dag   → tous les jours, recalcule staging + curated
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import numpy as np
import psycopg2
from elasticsearch import Elasticsearch
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# ─────────────────────────────────────────
# Connexions
# ─────────────────────────────────────────
ES_URL = "http://elasticsearch:9200"
PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "database": "crypto_lake",
    "user": "nathan",
    "password": "password"
}

default_args = {
    "owner": "nathan",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}


# ─────────────────────────────────────────
# TÂCHES
# ─────────────────────────────────────────

def task_ingest_coingecko(**context):
    """Ingère le prix live depuis CoinGecko dans Elasticsearch."""
    es = Elasticsearch(ES_URL)

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    btc = data["bitcoin"]

    doc = {
        "timestamp": datetime.utcnow().isoformat(),
        "price_usd": btc.get("usd"),
        "market_cap": btc.get("usd_market_cap"),
        "volume_24h": btc.get("usd_24h_vol"),
        "price_change_24h": btc.get("usd_24h_change"),
        "raw_payload": json.dumps(data),
        "ingested_at": datetime.utcnow().isoformat()
    }

    es.index(index="raw_btc_live", body=doc)

    # XCom : on pousse le prix pour la tâche suivante
    context["ti"].xcom_push(key="btc_price", value=btc.get("usd"))
    print(f"Prix ingéré : ${btc.get('usd'):,.2f}")


def task_log_price(**context):
    """Récupère le prix depuis XCom et le logue."""
    ti = context["ti"]
    price = ti.xcom_pull(task_ids="ingest_coingecko", key="btc_price")
    print(f"Prix BTC récupéré via XCom : ${price:,.2f}")


def task_run_staging(**context):
    """
    Version légère pour le DAG daily :
    Ajoute uniquement le prix du jour depuis CoinGecko dans staging.
    Le staging historique complet est géré par le script manuel.
    """
    import requests
    from datetime import datetime, date
    import psycopg2

    conn = psycopg2.connect(**PG_CONN)

    # Récupère le prix actuel depuis CoinGecko
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()["bitcoin"]

    today = datetime.utcnow().date()
    price = data.get("usd")
    volume = data.get("usd_24h_vol")

    # Récupère les dernières valeurs de staging pour calculer les indicateurs
    with conn.cursor() as cur:
        cur.execute("""
            SELECT close, returns FROM staging_btc
            ORDER BY timestamp DESC LIMIT 30
        """)
        rows = cur.fetchall()

    if not rows or price is None:
        print("Pas assez de données pour mettre à jour le staging")
        conn.close()
        return

    closes = [r[0] for r in rows]
    closes.insert(0, price)  # Ajoute le prix du jour en tête

    # Calcul simplifié des retours
    returns = (closes[0] - closes[1]) / closes[1] if closes[1] else 0

    # Volatilité 7j
    recent_returns = []
    for i in range(min(7, len(closes) - 1)):
        ret = (closes[i] - closes[i+1]) / closes[i+1] if closes[i+1] else 0
        recent_returns.append(ret)
    volatility_7d = float(np.std(recent_returns)) if recent_returns else 0

    # RSI simplifié
    gains = [r for r in recent_returns if r > 0]
    losses = [-r for r in recent_returns if r < 0]
    avg_gain = np.mean(gains) if gains else 0
    avg_loss = np.mean(losses) if losses else 0.001
    rsi = 100 - (100 / (1 + avg_gain / avg_loss))

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO staging_btc
                (timestamp, open, high, low, close, volume,
                 rsi, bb_upper, bb_lower, bb_middle,
                 volatility_7d, volatility_30d, returns)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (timestamp) DO UPDATE SET
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                rsi = EXCLUDED.rsi,
                volatility_7d = EXCLUDED.volatility_7d,
                returns = EXCLUDED.returns
        """, (
            datetime.combine(today, datetime.min.time()),
            price, price, price, price, volume or 0,
            rsi, price * 1.02, price * 0.98, price,
            volatility_7d, volatility_7d * 2, returns
        ))
        conn.commit()

    print(f"Staging mis à jour pour aujourd'hui : ${price:,.2f}")
    conn.close()


def task_run_curated(**context):
    """Recalcule les régimes KMeans et met à jour la zone curated."""
    conn = psycopg2.connect(**PG_CONN)

    df = pd.read_sql("""
        SELECT timestamp, close, rsi, volatility_7d,
               volatility_30d, returns, bb_upper, bb_lower
        FROM staging_btc ORDER BY timestamp ASC
    """, conn)

    features = ["rsi", "volatility_7d", "volatility_30d", "returns"]
    X_scaled = StandardScaler().fit_transform(df[features])
    df["regime"] = KMeans(n_clusters=3, random_state=42, n_init=10).fit_predict(X_scaled)

    cluster_stats = {}
    for cid in range(3):
        mask = df["regime"] == cid
        cluster_stats[cid] = {
            "returns": df.loc[mask, "returns"].mean(),
            "volatility": df.loc[mask, "volatility_7d"].mean()
        }

    sorted_clusters = sorted(cluster_stats.items(), key=lambda x: x[1]["returns"])
    label_map = {
        sorted_clusters[0][0]: "bear",
        sorted_clusters[1][0]: "sideways",
        sorted_clusters[2][0]: "bull"
    }
    df["regime_label"] = df["regime"].map(label_map)

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE curated_btc RESTART IDENTITY")
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO curated_btc
                    (timestamp, close, rsi, volatility_7d, volatility_30d,
                     returns, bb_upper, bb_lower, regime, regime_label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (timestamp) DO NOTHING
            """, (
                row["timestamp"], row["close"], row["rsi"],
                row["volatility_7d"], row["volatility_30d"],
                row["returns"], row["bb_upper"], row["bb_lower"],
                int(row["regime"]), row["regime_label"]
            ))
        conn.commit()

    print(f"Curated mis à jour : {len(df)} enregistrements")
    conn.close()


# ─────────────────────────────────────────
# DAG 1 : Ingestion live toutes les heures
# ─────────────────────────────────────────
with DAG(
    dag_id="ingest_live_dag",
    default_args=default_args,
    description="Ingère le prix BTC live depuis CoinGecko toutes les heures",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crypto", "ingestion"]
) as ingest_dag:

    ingest = PythonOperator(
        task_id="ingest_coingecko",
        python_callable=task_ingest_coingecko,
        provide_context=True
    )

    log_price = PythonOperator(
        task_id="log_price",
        python_callable=task_log_price,
        provide_context=True
    )

    ingest >> log_price


# ─────────────────────────────────────────
# DAG 2 : Pipeline complet tous les jours
# ─────────────────────────────────────────
with DAG(
    dag_id="full_pipeline_dag",
    default_args=default_args,
    description="Recalcule staging et curated chaque jour à minuit",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crypto", "pipeline"]
) as pipeline_dag:

    staging = PythonOperator(
        task_id="run_staging",
        python_callable=task_run_staging,
        provide_context=True
    )

    curated = PythonOperator(
        task_id="run_curated",
        python_callable=task_run_curated,
        provide_context=True
    )

    staging >> curated