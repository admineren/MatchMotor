import os
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException, Query, Header
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -----------------------------
# Config
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("matchmotor")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise ValueError("DATABASE_URL env missing")

# Nosy
NOSY_BASE_URL = os.getenv("NOSY_BASE_URL", "https://www.nosyapi.com/apiv2/service").strip().rstrip("/")
NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()

# Nosy'de odds ve results ayrı apiID olabiliyor
NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID", "").strip()        # örn: 1881134
NOSY_RESULTS_API_ID = os.getenv("NOSY_RESULTS_API_ID", "").strip()  # örn: 1881149
# Check endpointi çoğu zaman results apiID ile çalışıyor
NOSY_CHECK_API_ID = (os.getenv("NOSY_CHECK_API_ID", "").strip() or NOSY_RESULTS_API_ID)

if not NOSY_BASE_URL.lower().startswith(("http://", "https://")):
    raise ValueError("NOSY_BASE_URL http(s) ile başlamalı. Örn: https://www.nosyapi.com/apiv2/service")

# Basit admin koruması (Render env'ye koy)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
)

app = FastAPI(title="MatchMotor API", version="0.2.0")


# -----------------------------
# Helpers
# -----------------------------
def require_admin(x_admin_token: Optional[str]) -> None:
    if not ADMIN_TOKEN:
        return  # token set edilmediyse admin açık kalsın
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def nosy_get(path: str, api_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    NosyAPI çağrısı.
    Dokümandaki kullanım: ?apiKey=...&apiID=...
    """
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env missing")
    if not api_id:
        raise HTTPException(status_code=500, detail="Nosy apiID env missing (NOSY_ODDS_API_ID / NOSY_RESULTS_API_ID)")

    url = f"{NOSY_BASE_URL}/{path.lstrip('/')}"
    q: Dict[str, Any] = {"apiKey": NOSY_API_KEY, "apiID": api_id}
    if params:
        q.update({k: v for k, v in params.items() if v is not None})

    try:
        r = requests.get(url, params=q, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        body_preview = (r.text or "")[:500]
        raise HTTPException(status_code=502, detail=f"NosyAPI HTTP error: {e}. Response: {body_preview}")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI request failed: {e}")


def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


def init_db() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY,
            match_id INTEGER UNIQUE NOT NULL,
            datetime TIMESTAMP NULL,
            league TEXT NULL,
            country TEXT NULL,
            team1 TEXT NULL,
            team2 TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS match_odds (
            id SERIAL PRIMARY KEY,
            match_id INTEGER UNIQUE NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS match_results (
            id SERIAL PRIMARY KEY,
            match_id INTEGER UNIQUE NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """))

        # Indexler (hata olursa uygulama çökmesin)
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league);"))
        except Exception as e:
            logger.warning("idx_matches_league create failed: %s", e)

        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_matches_datetime ON matches(datetime);"))
        except Exception as e:
            logger.warning("idx_matches_datetime create failed: %s", e)


@app.on_event("startup")
def _startup():
    init_db()


# -----------------------------
# Basic API
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}


@app.get("/matches")
def list_matches(limit: int = 200, offset: int = 0):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
            SELECT match_id, datetime, league, country, team1, team2
            FROM matches
            ORDER BY datetime NULLS LAST
            LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        ).mappings().all()
        return {"count": len(rows), "data": list(rows)}


@app.post("/admin/matches/clear")
def clear_matches(x_admin_token: Optional[str] = Header(default=None)):
    require_admin(x_admin_token)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE match_odds RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE match_results RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE matches RESTART IDENTITY;"))
    return {"ok": True}


# -----------------------------
# Nosy passthrough + DB upsert
# -----------------------------
@app.get("/nosy-check")
def nosy_check():
    # Endpoint: nosy-service/check
    # NOTE: Check için genelde RESULTS apiID gerekir (1881149). O yüzden NOSY_CHECK_API_ID kullanıyoruz.
    return nosy_get("nosy-service/check", NOSY_CHECK_API_ID)


@app.get("/nosy-matches-by-date")
def nosy_matches_by_date(date: str = Query(..., description="YYYY-MM-DD")):
    """
    Endpoint: bettable-matches/date
    Bu endpointten gelen MatchID'leri DB'ye upsert ederiz.
    """
    payload = nosy_get("bettable-matches/date", NOSY_ODDS_API_ID, {"date": date})

    data = payload.get("data") or []
    upserted = 0

    if isinstance(data, list):
        with engine.begin() as conn:
            for row in data:
                mid = row.get("MatchID") or row.get("match_id") or row.get("matchId")
                if mid is None:
                    continue

                try:
                    mid_int = int(mid)
                except Exception:
                    continue

                dt_raw = row.get("DateTime") or row.get("datetime") or row.get("dateTime")
                dt_val = None
                if isinstance(dt_raw, str) and dt_raw.strip():
                    try:
                        dt_val = datetime.fromisoformat(dt_raw.strip().replace(" ", "T"))
                    except Exception:
                        dt_val = None

                conn.execute(
                    text("""
                    INSERT INTO matches (match_id, datetime, league, country, team1, team2, updated_at)
                    VALUES (:match_id, :dt, :league, :country, :team1, :team2, NOW())
                    ON CONFLICT (match_id) DO UPDATE
                    SET datetime = COALESCE(EXCLUDED.datetime, matches.datetime),
                        league   = COALESCE(EXCLUDED.league,   matches.league),
                        country  = COALESCE(EXCLUDED.country,  matches.country),
                        team1    = COALESCE(EXCLUDED.team1,    matches.team1),
                        team2    = COALESCE(EXCLUDED.team2,    matches.team2),
                        updated_at = NOW()
                    """),
                    {
                        "match_id": mid_int,
                        "dt": dt_val,
                        "league": row.get("League"),
                        "country": row.get("Country"),
                        "team1": row.get("Team1"),
                        "team2": row.get("Team2"),
                    },
                )
                upserted += 1

    payload["upserted"] = upserted
    return payload


@app.get("/nosy-opening-odds")
def nosy_opening_odds(match_id: int = Query(..., description="Nosy MatchID")):
    """
    Endpoint: bettable-matches/opening-odds
    Dönen payload'u match_odds tablosuna yazar.
    """
    payload = nosy_get("bettable-matches/opening-odds", NOSY_ODDS_API_ID, {"match_id": match_id})

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO match_odds (match_id, payload, updated_at)
            VALUES (:match_id, :payload::jsonb, NOW())
            ON CONFLICT (match_id) DO UPDATE
            SET payload = EXCLUDED.payload, updated_at = NOW()
            """),
            {"match_id": match_id, "payload": json_dumps(payload)},
        )

    return payload


@app.get("/nosy-result-details")
def nosy_result_details(match_id: int = Query(..., description="Nosy MatchID")):
    """
    Endpoint: bettable-result/details
    Dönen payload'u match_results tablosuna yazar.
    """
    payload = nosy_get("bettable-result/details", NOSY_RESULTS_API_ID, {"match_id": match_id})

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO match_results (match_id, payload, updated_at)
            VALUES (:match_id, :payload::jsonb, NOW())
            ON CONFLICT (match_id) DO UPDATE
            SET payload = EXCLUDED.payload, updated_at = NOW()
            """),
            {"match_id": match_id, "payload": json_dumps(payload)},
        )

    return payload
