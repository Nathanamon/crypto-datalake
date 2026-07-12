# Crypto Data Lake

Pipeline de données end-to-end pour l'analyse de marché Bitcoin.
Ingestion, transformation, clustering de régimes de marché et exposition via API.

## Architecture
Raw (Elasticsearch) → Staging (PostgreSQL) → Curated (PostgreSQL) → API (FastAPI)

| Zone | Technologie | Contenu |
|---|---|---|
| Raw | Elasticsearch | 7.6M entrées OHLCV minute + prix live CoinGecko |
| Staging | PostgreSQL | Données daily + indicateurs techniques (RSI, Bollinger, volatilité) |
| Curated | PostgreSQL | Régimes de marché classifiés (bull/bear/sideways) via KMeans |

## Sources de données

- **Fichier** : [Bitcoin Historical Data](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data) (Kaggle) — données OHLCV à la minute depuis 2012
- **API** : [CoinGecko API](https://www.coingecko.com/en/api) — prix live, market cap, volume (gratuite, sans clé)

## Stack technique

- **Elasticsearch 8.11** — stockage zone raw
- **PostgreSQL 15** — stockage zones staging et curated
- **Apache Airflow 2.8** — orchestration et scheduling
- **FastAPI** — API Gateway
- **Scikit-learn** — KMeans clustering (détection de régimes de marché)
- **Docker / Docker Compose** — containerisation

## Installation et démarrage

### Prérequis
- Docker Desktop installé et démarré
- Python 3.10+
- Compte Kaggle (pour télécharger le dataset)

### 1. Clone le repo

```bash
git clone <url-du-repo>
cd crypto-datalake
```

### 2. Télécharge le dataset Kaggle

Télécharge le fichier CSV depuis :
https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data

Place le fichier dans `data/raw/btc_historical.csv`

### 3. Démarre les services

```bash
docker-compose up -d
```

Attends 30-60 secondes que Elasticsearch et PostgreSQL démarrent.

### 4. Installe les dépendances Python

```bash
pip install -r requirements.txt
```

### 5. Lance le pipeline complet

```bash
# Zone Raw : ingestion CSV + API CoinGecko
python ingestion/ingest_raw.py

# Zone Staging : agrégation daily + indicateurs techniques
python staging/transform.py

# Zone Curated : clustering KMeans
python curated/clustering.py
```

### 6. Lance l'API

```bash
cd api
uvicorn main:app --reload --port 8000
```

## API Gateway

Base URL : `http://localhost:8000`

| Endpoint | Description |
|---|---|
| `GET /health` | État de tous les services |
| `GET /stats` | Métriques de remplissage par zone |
| `GET /raw?limit=10` | Dernières entrées brutes (Elasticsearch) |
| `GET /raw/live` | Dernier prix live CoinGecko |
| `GET /staging?limit=10` | Données avec indicateurs techniques |
| `GET /curated?limit=10` | Données avec régimes de marché |
| `GET /curated/current-regime` | Régime de marché actuel |

Documentation interactive : `http://localhost:8000/docs`

## Orchestration Airflow

Interface : `http://localhost:8080` (admin / admin)

| DAG | Schedule | Description |
|---|---|---|
| `ingest_live_dag` | @hourly | Ingère le prix BTC live depuis CoinGecko |
| `full_pipeline_dag` | @daily | Recalcule staging + curated |

Le DAG `ingest_live_dag` utilise **XCom** pour passer le prix BTC entre les tâches `ingest_coingecko` et `log_price`.

## ML — Détection de régimes de marché

Le pipeline curated applique un **KMeans clustering (k=3)** sur les features suivantes :
- RSI (Relative Strength Index)
- Volatilité glissante 7 jours
- Volatilité glissante 30 jours
- Retours journaliers

Les données sont normalisées via `StandardScaler` avant le clustering.
Les 3 clusters sont automatiquement labellisés **bull / bear / sideways**
en fonction des retours moyens de chaque cluster.

### Résultats sur données 2012-2026

| Régime | Périodes | Retour moyen | Volatilité 7j |
|---|---|---|---|
| Bull | 2093 jours | +0.014 | 0.069 |
| Bear | 2594 jours | -0.009 | 0.069 |
| Sideways | 590 jours | +0.011 | 0.203 |


## Choix techniques

**Pourquoi Elasticsearch pour la zone Raw ?**
Elasticsearch est optimisé pour l'indexation et la recherche de grands volumes de documents JSON non structurés. Avec 7.6M de points de données à la minute, il permet une ingestion rapide par batch et des requêtes flexibles sur les données brutes.

**Pourquoi PostgreSQL pour Staging et Curated ?**
Les zones staging et curated contiennent des données structurées et agrégées (daily) sur lesquelles on effectue des requêtes SQL analytiques. PostgreSQL est le choix naturel pour ce type de données relationnelles.

**Pourquoi KMeans pour la détection de régimes ?**
KMeans est un algorithme de clustering non supervisé accessible et interprétable. Avec k=3 clusters et une normalisation préalable des features, il permet de distinguer naturellement les phases de marché sans avoir besoin de données labellisées.