"""
Script d'ingestion Raw
- Source fichier : CSV historique Bitcoin (Kaggle)
- Source API : CoinGecko (prix live)
Les données brutes sont stockées dans Elasticsearch SANS transformation.
"""

import pandas as pd
import requests
import json
from datetime import datetime
from elasticsearch import Elasticsearch

# Connexion Elasticsearch
es = Elasticsearch("http://localhost:9200")

def create_indices():
    """Crée les index Elasticsearch pour la zone raw."""
    
    # Index pour les données historiques (CSV)
    if not es.indices.exists(index="raw_btc_historical"):
        es.indices.create(index="raw_btc_historical", body={
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "open": {"type": "float"},
                    "high": {"type": "float"},
                    "low": {"type": "float"},
                    "close": {"type": "float"},
                    "volume": {"type": "float"},
                    "ingested_at": {"type": "date"}
                }
            }
        })
        print("Index raw_btc_historical créé")

    # Index pour les données live (API)
    if not es.indices.exists(index="raw_btc_live"):
        es.indices.create(index="raw_btc_live", body={
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "price_usd": {"type": "float"},
                    "market_cap": {"type": "float"},
                    "volume_24h": {"type": "float"},
                    "price_change_24h": {"type": "float"},
                    "ingested_at": {"type": "date"}
                }
            }
        })
        print("Index raw_btc_live créé")


def ingest_historical_csv(filepath: str):
    """
    Ingère le CSV historique Kaggle dans Elasticsearch.
    On stocke les données BRUTES sans aucune transformation.
    """
    print(f"Lecture du fichier {filepath}...")
    
    try:
        df = pd.read_csv(filepath)
        print(f"{len(df)} lignes trouvées")
    except FileNotFoundError:
        print(f"Fichier introuvable : {filepath}")
        return

    # Renommage des colonnes pour uniformiser
    df.columns = [c.lower().strip() for c in df.columns]
    
    # Conversion du timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    elif "date" in df.columns:
        df["timestamp"] = pd.to_datetime(df["date"])
        df = df.drop(columns=["date"])

    # Suppression des lignes sans prix de clôture
    df = df.dropna(subset=["close"])
    
    # Ingestion par batch de 500 pour ne pas surcharger ES
    batch_size = 500
    total = 0
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        actions = []
        
        for _, row in batch.iterrows():
            doc = {
                "timestamp": row["timestamp"].isoformat(),
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row["close"]),
                "volume": float(row.get("volume_(btc)", row.get("volume", 0))),
                "ingested_at": datetime.utcnow().isoformat()
            }
            actions.append(doc)
        
        # Bulk insert
        bulk_body = []
        for doc in actions:
            bulk_body.append({"index": {"_index": "raw_btc_historical"}})
            bulk_body.append(doc)
        
        es.bulk(body=bulk_body)
        total += len(batch)
        print(f"  {total}/{len(df)} lignes ingérées...")

    print(f"Ingestion historique terminée : {total} documents dans Elasticsearch")


def ingest_live_coingecko():
    """
    Appelle l'API CoinGecko et stocke le payload brut dans Elasticsearch.
    Aucune transformation — on garde exactement ce que l'API renvoie.
    """
    print("Appel CoinGecko API...")
    
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur API CoinGecko : {e}")
        return

    # Extraction des données brutes
    btc_data = data.get("bitcoin", {})
    
    doc = {
        "timestamp": datetime.utcnow().isoformat(),
        "price_usd": btc_data.get("usd"),
        "market_cap": btc_data.get("usd_market_cap"),
        "volume_24h": btc_data.get("usd_24h_vol"),
        "price_change_24h": btc_data.get("usd_24h_change"),
        "raw_payload": json.dumps(data),  # On garde le payload complet
        "ingested_at": datetime.utcnow().isoformat()
    }
    
    es.index(index="raw_btc_live", body=doc)
    print(f"Prix live ingéré : ${btc_data.get('usd'):,.2f}")


if __name__ == "__main__":
    print("Démarrage ingestion Raw...")
    create_indices()
    ingest_historical_csv("data/raw/btc_historical.csv")
    ingest_live_coingecko()
    print("Ingestion Raw terminée !")