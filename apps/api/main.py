import os
import json
import datetime as dt
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -----------------------------------------------------------------------------
# Config (ENV)
# -----------------------------------------------------------------------------
# NosyAPI
NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()

# Nosy panelinde görünen "Base URL" iki farklı kök içeriyor:
# - Service endpointleri:  https://www.nosyapi.com/apiv2/service/...
# - Check endpointi:       https://www.nosyapi.com/apiv2/nosy-service/check
#
# Bu yüzden iki base tanımlıyoruz.
NOSY_SERVICE_BASE_URL = os.getenv("NOSY_SERVICE_BASE_URL", "https://www.nosyapi.com/apiv2/service/").strip()
NOSY_ROOT_BASE_URL = os.getenv("NOSY_ROOT_BASE_URL", "https://www.nosyapi.com/apiv2/").strip()

# İKİ ayrı API ID:
# - ORAN / PROGRAM API (bettable-matches/* + nosy-service/check)
# - SONUÇ API (bettable-result/* + nosy-service/check)
NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID", "").strip()       # ör: 1881134
NOSY_RESULTS_API_ID = os.getenv("NOSY_RESULTS_API_ID", "").strip() # ör: 1881149

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./app.db").strip()

# Render Postgres bazen "postgres://" döner, SQLAlchemy "postgresql://" ister.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _normalize_base_url(base: str) -> str:
    base = (base or "").strip()
    if not base:
        return ""
    if not base.startswith("http"):
        base = "https://" + base.lstrip("/")
    if not base.endswith("/"):
        base += "/"
    return base

NOSY_SERVICE_BASE_URL = _normalize_base_url(NOSY_SERVICE_BASE_URL) or "https://www.nosyapi.com/apiv2/service/"
NOSY_ROOT_BASE_URL = _normalize_base_url(NOSY_ROOT_BASE_URL) or "https://www.nosyapi.com/apiv2/"

def _join_url(base: str, endpoint: str) -> str:
    endpoint = endpoint.lstrip("/")
    return base + endpoint

def _pick_api_id(api_kind: str) -> str:
    """
    api_kind:
      - "odds"    -> NOSY_ODDS_API_ID
      - "results" -> NOSY_RESULTS_API_ID
    """
    if api_kind == "odds":
        return NOSY_ODDS_API_ID
    if api_kind == "results":
        return NOSY_RESULTS_API_ID
    return ""

def _dump_json(obj: Any) -> str:
    # DB'de düzgün JSON saklamak için
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def nosy_call(endpoint: str, *, params: Optional[Dict[str, Any]] = None, api_kind: str = "odds") -> Dict[str, Any]:
    """
    NosyAPI'ye çağrı yapar.
    - endpoint: 'bettable-matches/date' gibi
    - api_kind: odds/results
    Not: 'nosy-service/check' endpointi /service altında değil, ROOT base ile çağrılır.
    """
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env eksik.")
    api_id = _pick_api_id(api_kind)
    if not api_id:
        raise HTTPException(status_code=500, detail="NOSY_ODDS_API_ID veya NOSY_RESULTS_API_ID env eksik / yanlış.")

    # Check endpointi root base ister, diğerleri service base ister
    is_check = endpoint.lstrip("/").startswith("nosy-service/check")
    base = NOSY_ROOT_BASE_URL if is_check else NOSY_SERVICE_BASE_URL
    url = _join_url(base, endpoint)

    q = dict(params or {})
    # Nosy dokümanında param adları: apiKey + apiID
    q["apiKey"] = NOSY_API_KEY
    q["apiID"] = api_id

    try:
        r = requests.get(url, params=q, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI bağlantı hatası: {e}")

    # Nosy bazen 200 dönüp status=failure verir. O yüzden raise_for_status yapmıyoruz.
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise HTTPException(status_code=r.status_code, detail={"url": str(r.url), "body": body})

    try:
        return r.json()
    except Exception:
        raise HTTPException(status_code=502, detail={"url": str(r.url), "body": r.text})

# -----------------------------------------------------------------------------
# DB init (cross-db schema: SQLite + Postgres)
# -----------------------------------------------------------------------------
engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def ensure_schema() -> None:
    dialect = engine.dialect.name  # "sqlite" | "postgresql" | ...
    is_sqlite = dialect == "sqlite"

    # Postgres'ta AUTOINCREMENT yok -> Render logundaki hatanın sebebi bu.
    # - SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
    # - Postgres: BIGSERIAL (veya GENERATED ... IDENTITY)
    odds_id_col = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"
    res_id_col = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"

    with engine.begin() as conn:
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS matches (
            match_id      BIGINT PRIMARY KEY,
            date          TEXT,
            time          TEXT,
            datetime      TEXT,
            league        TEXT,
            country       TEXT,
            team1         TEXT,
            team2         TEXT,
            raw_json      TEXT
        );
        """))
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS match_odds (
            id            {odds_id_col},
            match_id      BIGINT,
            fetched_at    TEXT,
            raw_json      TEXT
        );
        """))
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS match_results (
            id            {res_id_col},
            match_id      BIGINT,
            fetched_at    TEXT,
            raw_json      TEXT
        );
        """))
        conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_match_odds_match_id
        ON match_odds(match_id);
        """))
        conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_match_results_match_id
        ON match_results(match_id);
        """))
        
ensure_schema()

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="MatchMotor API", version="0.3.0")

@app.get("/health")
def health():
    return {"ok": True, "time": dt.datetime.utcnow().isoformat()}

@app.get("/matches")
def list_matches(limit: int = 50):
    limit = max(1, min(500, limit))
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT match_id, datetime, league, team1, team2
            FROM matches
            ORDER BY datetime DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
    return {"count": len(rows), "data": list(rows)}

@app.post("/admin/matches/clear")
def clear_matches():
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM match_odds;"))
        conn.execute(text("DELETE FROM match_results;"))
        conn.execute(text("DELETE FROM matches;"))
    return {"ok": True}

# -----------------------------------------------------------------------------
# Nosy checks (iki API için ayrı)
# -----------------------------------------------------------------------------
@app.get("/nosy-check-odds")
def nosy_check_odds():
    return nosy_call("nosy-service/check", api_kind="odds")

@app.get("/nosy-check-results")
def nosy_check_results():
    return nosy_call("nosy-service/check", api_kind="results")

# Eski endpoint kalsın: varsayılan odds check döner
@app.get("/nosy-check")
def nosy_check():
    return nosy_call("nosy-service/check", api_kind="odds")

# -----------------------------------------------------------------------------
# Nosy endpoints
# -----------------------------------------------------------------------------
@app.get("/nosy-matches-by-date")
def nosy_matches_by_date(date: str = Query(..., description="YYYY-MM-DD")):
    payload = nosy_call("bettable-matches/date", params={"date": date}, api_kind="odds")
    data = payload.get("data") or []
    upserted = 0

    with engine.begin() as conn:
        for item in data:
            try:
                match_id = int(item.get("MatchID") or item.get("match_id") or item.get("matchId"))
            except Exception:
                continue

            dt_val = item.get("DateTime") or item.get("datetime") or ""
            time_val = item.get("Time") or item.get("time") or ""
            date_val = item.get("Date") or item.get("date") or ""
            league = item.get("League") or item.get("league") or ""
            country = item.get("Country") or item.get("country") or ""
            team1 = item.get("Team1") or item.get("team1") or ""
            team2 = item.get("Team2") or item.get("team2") or ""

            conn.execute(text("""
                INSERT INTO matches(match_id, date, time, datetime, league, country, team1, team2, raw_json)
                VALUES(:match_id, :date, :time, :datetime, :league, :country, :team1, :team2, :raw_json)
                ON CONFLICT(match_id) DO UPDATE SET
                    date=excluded.date,
                    time=excluded.time,
                    datetime=excluded.datetime,
                    league=excluded.league,
                    country=excluded.country,
                    team1=excluded.team1,
                    team2=excluded.team2,
                    raw_json=excluded.raw_json
            """), {
                "match_id": match_id,
                "date": str(date_val),
                "time": str(time_val),
                "datetime": str(dt_val),
                "league": str(league),
                "country": str(country),
                "team1": str(team1),
                "team2": str(team2),
                "raw_json": _dump_json(item),
            })
            upserted += 1

    return {"ok": True, "date": date, "upserted": upserted, "nosy": payload}

@app.get("/nosy-opening-odds")
def nosy_opening_odds(match_id: int = Query(..., description="Nosy MatchID")):
    payload = nosy_call(
        "bettable-matches/opening-odds",
        params={"matchID": match_id},
        api_kind="odds",
    )

    now = dt.datetime.utcnow().isoformat()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO match_odds (match_id, fetched_at, raw_json)
            VALUES (:match_id, :fetched_at, :raw_json)
            ON CONFLICT (match_id)
            DO UPDATE SET fetched_at = EXCLUDED.fetched_at,
                          raw_json  = EXCLUDED.raw_json
        """), {
            "match_id": match_id,
            "fetched_at": now,
            "raw_json": _dump_json(payload),
        })

    return {
        "ok": True,
        "match_id": match_id,
        "saved": True,
        "nosy": payload
    }


@app.get("/nosy-results")
def nosy_results(match_id: int = Query(..., description="Nosy MatchID")):
    return nosy_call("bettable-result", params={"matchID": match_id}, api_kind="results")

@app.get("/nosy-result-details")
def nosy_result_details(match_id: int = Query(..., description="Nosy MatchID")):
    payload = nosy_call("bettable-result/details", params={"matchID": match_id}, api_kind="results")

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO match_results(match_id, fetched_at, raw_json)
            VALUES(:match_id, :fetched_at, :raw_json)
        """), {
            "match_id": match_id,
            "fetched_at": dt.datetime.utcnow().isoformat(),
            "raw_json": _dump_json(payload),
        })

    return payload
