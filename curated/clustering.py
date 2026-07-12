"""
Pipeline Curated
- Lit les données depuis PostgreSQL (staging)
- Applique KMeans pour classifier les régimes de marché
- Stocke les résultats enrichis dans PostgreSQL (zone curated)
"""

import pandas as pd
import numpy as np
import psycopg2
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import warnings
warnings.filterwarnings("ignore")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="crypto_lake",
    user="nathan",
    password="password"
)


def create_curated_table():
    """Crée la table curated dans PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS curated_btc")
        cur.execute("""
            CREATE TABLE curated_btc (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP UNIQUE,
                close FLOAT,
                rsi FLOAT,
                volatility_7d FLOAT,
                volatility_30d FLOAT,
                returns FLOAT,
                bb_upper FLOAT,
                bb_lower FLOAT,
                regime INTEGER,
                regime_label VARCHAR(20),
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
    print("Table curated_btc recréée")


def fetch_staging_data() -> pd.DataFrame:
    """Récupère les données depuis la zone staging."""
    print("Récupération des données staging...")
    
    query = """
        SELECT timestamp, close, rsi, volatility_7d, 
               volatility_30d, returns, bb_upper, bb_lower
        FROM staging_btc
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, conn)
    print(f"{len(df)} enregistrements récupérés")
    return df


def label_regime(cluster_id: int, cluster_stats: dict) -> str:
    """
    Attribue un label lisible à chaque cluster KMeans.
    Seuils calibrés pour des retours journaliers crypto.
    """
    stats = cluster_stats[cluster_id]
    
    # On trie les clusters par retour moyen pour assigner les labels
    sorted_clusters = sorted(
        cluster_stats.items(),
        key=lambda x: x[1]["returns"]
    )
    
    # Le cluster avec le retour le plus bas → bear
    # Le cluster avec le retour le plus haut → bull
    # Le cluster du milieu → sideways
    if cluster_id == sorted_clusters[0][0]:
        return "bear"
    elif cluster_id == sorted_clusters[-1][0]:
        return "bull"
    else:
        return "sideways"

def apply_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique KMeans clustering pour détecter les régimes de marché.
    Features utilisées : RSI, volatilité 7j, volatilité 30j, retours
    """
    print("Application du KMeans clustering...")

    # Features pour le clustering
    features = ["rsi", "volatility_7d", "volatility_30d", "returns"]
    X = df[features].copy()

    # Normalisation — indispensable pour KMeans
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # KMeans avec 3 clusters : bull / bear / sideways
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    df["regime"] = kmeans.fit_predict(X_scaled)

    # Calcul des stats par cluster pour attribuer les labels
    cluster_stats = {}
    for cluster_id in range(3):
        mask = df["regime"] == cluster_id
        cluster_stats[cluster_id] = {
            "returns": df.loc[mask, "returns"].mean(),
            "volatility": df.loc[mask, "volatility_7d"].mean()
        }

    # Attribution des labels
    df["regime_label"] = df["regime"].apply(
        lambda x: label_regime(x, cluster_stats)
    )

    # Résumé des régimes détectés
    print("Résumé des régimes détectés :")
    for cluster_id in range(3):
        mask = df["regime"] == cluster_id
        label = df.loc[mask, "regime_label"].iloc[0]
        count = mask.sum()
        avg_return = cluster_stats[cluster_id]["returns"]
        avg_vol = cluster_stats[cluster_id]["volatility"]
        print(f"  Cluster {cluster_id} ({label}) → {count} périodes | "
              f"retour moyen: {avg_return:.4f} | volatilité: {avg_vol:.4f}")

    return df


def load_curated(df: pd.DataFrame):
    """Charge les données enrichies dans la table curated."""
    print("Chargement dans PostgreSQL (curated)...")

    with conn.cursor() as cur:
        inserted = 0
        for _, row in df.iterrows():
            try:
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
                inserted += 1
            except Exception as e:
                print(f"Erreur : {e}")
                conn.rollback()
                continue

        conn.commit()
    print(f"{inserted} enregistrements chargés dans curated_btc")


if __name__ == "__main__":
    print("Démarrage pipeline Curated...")
    create_curated_table()
    df = fetch_staging_data()
    if not df.empty:
        df = apply_kmeans(df)
        load_curated(df)
    print("Pipeline Curated terminé !")