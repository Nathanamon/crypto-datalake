"""
API Gateway — Crypto Data Lake
Endpoints :
  /health   → état des services
  /stats    → métriques des zones
  /raw      → données brutes (Elasticsearch)
  /staging  → données transformées (PostgreSQL)
  /curated  → régimes de marché (PostgreSQL)
"""

from fastapi import FastAPI, HTTPException, Query
from elasticsearch import Elasticsearch
import psycopg2
import psycopg2.extras
from datetime import datetime
import os

app = FastAPI(
    title="Crypto Data Lake API",
    description="API Gateway pour accéder aux zones raw, staging et curated du data lake crypto",
    version="1.0.0"
)

# Connexions
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://nathan:password@localhost/crypto_lake")

es = Elasticsearch(ES_URL)


def get_pg_conn():
    """Retourne une connexion PostgreSQL."""
    return psycopg2.connect(POSTGRES_URL)


# ─────────────────────────────────────────
# /health
# ─────────────────────────────────────────
@app.get("/health")
def health():
    """Vérifie l'état de tous les services."""
    status = {
        "api": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "elasticsearch": "unknown",
        "postgresql": "unknown"
    }

    # Check Elasticsearch
    try:
        if es.ping():
            status["elasticsearch"] = "ok"
        else:
            status["elasticsearch"] = "down"
    except Exception as e:
        status["elasticsearch"] = f"error: {str(e)}"

    # Check PostgreSQL
    try:
        conn = get_pg_conn()
        conn.close()
        status["postgresql"] = "ok"
    except Exception as e:
        status["postgresql"] = f"error: {str(e)}"

    return status


# ─────────────────────────────────────────
# /stats
# ─────────────────────────────────────────
@app.get("/stats")
def stats():
    """Métriques sur le remplissage de chaque zone du data lake."""
    result = {}

    # Stats Elasticsearch
    try:
        hist_count = es.count(index="raw_btc_historical")["count"]
        live_count = es.count(index="raw_btc_live")["count"]
        result["raw"] = {
            "btc_historical_documents": hist_count,
            "btc_live_documents": live_count,
            "storage": "elasticsearch"
        }
    except Exception as e:
        result["raw"] = {"error": str(e)}

    # Stats PostgreSQL
    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM staging_btc")
            row = cur.fetchone()
            result["staging"] = {
                "total_records": row[0],
                "from": str(row[1]),
                "to": str(row[2]),
                "storage": "postgresql"
            }

            cur.execute("""
                SELECT COUNT(*), 
                       COUNT(CASE WHEN regime_label='bull' THEN 1 END) as bull,
                       COUNT(CASE WHEN regime_label='bear' THEN 1 END) as bear,
                       COUNT(CASE WHEN regime_label='sideways' THEN 1 END) as sideways
                FROM curated_btc
            """)
            row = cur.fetchone()
            result["curated"] = {
                "total_records": row[0],
                "bull_periods": row[1],
                "bear_periods": row[2],
                "sideways_periods": row[3],
                "storage": "postgresql"
            }
        conn.close()
    except Exception as e:
        result["postgresql"] = {"error": str(e)}

    return result


# ─────────────────────────────────────────
# /raw
# ─────────────────────────────────────────
@app.get("/raw")
def raw(limit: int = Query(default=10, le=100)):
    """
    Retourne les dernières entrées brutes depuis Elasticsearch.
    Paramètre : limit (max 100, défaut 10)
    """
    try:
        result = es.search(
            index="raw_btc_historical",
            body={
                "query": {"match_all": {}},
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": limit
            }
        )
        hits = [hit["_source"] for hit in result["hits"]["hits"]]
        return {
            "zone": "raw",
            "source": "elasticsearch",
            "count": len(hits),
            "data": hits
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/raw/live")
def raw_live():
    """Retourne le dernier prix live ingéré depuis CoinGecko."""
    try:
        result = es.search(
            index="raw_btc_live",
            body={
                "query": {"match_all": {}},
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": 1
            }
        )
        hits = result["hits"]["hits"]
        if not hits:
            raise HTTPException(status_code=404, detail="Aucune donnée live trouvée")
        return {
            "zone": "raw",
            "source": "coingecko_api",
            "data": hits[0]["_source"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# /staging
# ─────────────────────────────────────────
@app.get("/staging")
def staging(limit: int = Query(default=10, le=100)):
    """
    Retourne les données transformées avec indicateurs techniques.
    Paramètre : limit (max 100, défaut 10)
    """
    try:
        conn = get_pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT timestamp, open, high, low, close, volume,
                       rsi, bb_upper, bb_lower, bb_middle,
                       volatility_7d, volatility_30d, returns
                FROM staging_btc
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        conn.close()
        return {
            "zone": "staging",
            "source": "postgresql",
            "count": len(rows),
            "data": [dict(row) for row in rows]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# /curated
# ─────────────────────────────────────────
@app.get("/curated")
def curated(limit: int = Query(default=10, le=100)):
    """
    Retourne les données enrichies avec les régimes de marché détectés.
    Paramètre : limit (max 100, défaut 10)
    """
    try:
        conn = get_pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT timestamp, close, rsi, volatility_7d,
                       volatility_30d, returns, regime, regime_label
                FROM curated_btc
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        conn.close()
        return {
            "zone": "curated",
            "source": "postgresql",
            "count": len(rows),
            "data": [dict(row) for row in rows]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/curated/current-regime")
def current_regime():
    """Retourne le régime de marché actuel (dernière entrée)."""
    try:
        conn = get_pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT timestamp, close, rsi, volatility_7d,
                       regime, regime_label
                FROM curated_btc
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = cur.fetchone()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Aucune donnée curated trouvée")
        return {
            "current_regime": dict(row)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))