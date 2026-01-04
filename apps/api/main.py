import os
import json
import datetime as dt
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# Config (ENV)
# -----------------------------------------------------------------------------
NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()

# Base URL'ler
NOSY_SERVICE_BASE_URL = os.getenv("NOSY_SERVICE_BASE_URL", "https://www.nosyapi.com/apiv2/service/").strip()
NOSY_ROOT_BASE_URL = os.getenv("NOSY_ROOT_BASE_URL", "https://www.nosyapi.com/apiv2/").strip()

# İki ayrı API ID (odds/results)
NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID", "").strip()
NOSY_RESULTS_API_ID = os.getenv("NOSY_RESULTS_API_ID", "").strip()

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./app.db").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL.startswith("sqlite:"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, future=True)
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

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

def _join_url(base: str, endpoint: str) -> str:
    endpoint = endpoint.lstrip("/")
    return base + endpoint

def _pick_api_id(api_kind: str) -> str:
    if api_kind == "odds":
        return NOSY_ODDS_API_ID
    if api_kind == "results":
        return NOSY_RESULTS_API_ID
    return ""

def _dump_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

NOSY_SERVICE_BASE_URL = _normalize_base_url(NOSY_SERVICE_BASE_URL) or "https://www.nosyapi.com/apiv2/service/"
NOSY_ROOT_BASE_URL = _normalize_base_url(NOSY_ROOT_BASE_URL) or "https://www.nosyapi.com/apiv2/"

def nosy_call(endpoint: str, *, params: Optional[Dict[str, Any]] = None, api_kind: str = "odds") -> Dict[str, Any]:
    """
    endpoint: 'bettable-matches/date' gibi
    api_kind: 'odds' | 'results'
    """
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env eksik.")

    api_id = _pick_api_id(api_kind)
    ep = endpoint.lstrip("/")

    is_check = ep.startswith("nosy-service/check")

    # Service çağrılarında endpoint yanlışlıkla "service/..." gelirse temizle
    if (not is_check) and ep.startswith("service/"):
        ep = ep[len("service/"):]

    base = NOSY_ROOT_BASE_URL if is_check else NOSY_SERVICE_BASE_URL
    base = base.replace("/service/service", "/service")
    url = _join_url(base, ep)

    q = dict(params or {})
    q["apiKey"] = NOSY_API_KEY
    if api_id:
        q["apiID"] = api_id

    try:
        r = requests.get(url, params=q, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI bağlantı hatası: {e}")

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

def _extract_main_odds(opening_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    opening-odds payload'ından ana oranları çıkar:
    - ms1/ms0/ms2  (HomeWin/Draw/AwayWin)
    - alt25/ust25  (Under25/Over25)
    """
    try:
        data = opening_payload.get("data") or []
        if not data:
            return None, None, None, None, None
        item = data[0]
        ms1 = item.get("HomeWin")
        ms0 = item.get("Draw")
        ms2 = item.get("AwayWin")
        alt25 = item.get("Under25")
        ust25 = item.get("Over25")
        def f(x):
            if x is None or x == "":
                return None
            try:
                return float(x)
            except Exception:
                return None
        return f(ms1), f(ms0), f(ms2), f(alt25), f(ust25)
    except Exception:
        return None, None, None, None, None

def ensure_schema() -> None:
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    pk_ai = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"

    with engine.begin() as conn:
        # 1) Tüm Nosy maçları burada birikir
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS nosy_matches (
            nosy_match_id  BIGINT PRIMARY KEY,
            date           TEXT,
            time           TEXT,
            match_datetime TEXT,
            league         TEXT,
            country        TEXT,
            team1          TEXT,
            team2          TEXT,
            raw_json       TEXT
        );
        """))

        # 2) Filtreyi geçen maçlar (panelde göstereceğin "Matches")
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS matches (
            id             {pk_ai},
            nosy_match_id  BIGINT NOT NULL UNIQUE,
            match_datetime TEXT,
            league         TEXT,
            team1          TEXT,
            team2          TEXT,

            ms1            NUMERIC,
            ms0            NUMERIC,
            ms2            NUMERIC,
            alt25          NUMERIC,
            ust25          NUMERIC,

            ht_home        INT,
            ht_away        INT,
            ft_home        INT,
            ft_away        INT,

            created_at     TEXT,
            updated_at     TEXT
        );
        """))

        # 3) Odds/Results ham JSON
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS match_odds (
            id            {pk_ai},
            nosy_match_id BIGINT UNIQUE,
            fetched_at    TEXT,
            raw_json      TEXT,
            payload       TEXT
        );
        """))

        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS match_results (
            id            {pk_ai},
            nosy_match_id BIGINT UNIQUE,
            fetched_at    TEXT,
            raw_json      TEXT,
            payload       TEXT
        );
        """))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_nosy_matches_dt ON nosy_matches(match_datetime);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_matches_dt ON matches(datetime);"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_match_odds_nosy_match_id ON match_odds(nosy_match_id);"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_match_results_nosy_match_id ON match_results(nosy_match_id);"))

        if not is_sqlite:
            conn.execute(text("ALTER TABLE match_odds ADD COLUMN IF NOT EXISTS payload TEXT;"))
            conn.execute(text("ALTER TABLE match_results ADD COLUMN IF NOT EXISTS payload TEXT;"))
            conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS nosy_match_id BIGINT;"))

def _upsert_filtered_match_from_odds(conn, nosy_match_id: int, opening_payload: Dict[str, Any]) -> bool:
    ms1, ms0, ms2, alt25, ust25 = _extract_main_odds(opening_payload)

    # Filtre: 1X2 oranları yoksa "matches"e alma.
    if ms1 is None or ms0 is None or ms2 is None:
        return False

    row = conn.execute(
        text("""
            SELECT match_datetime, league, team1, team2
            FROM nosy_matches
            WHERE nosy_match_id = :mid
        """),
        {"mid": nosy_match_id},
    ).mappings().first()

    match_datetime = (row or {}).get("match_datetime") if row else None
    league = (row or {}).get("league") if row else None
    team1 = (row or {}).get("team1") if row else None
    team2 = (row or {}).get("team2") if row else None

    now = dt.datetime.utcnow().isoformat()

    conn.execute(
        text("""
            INSERT INTO matches(
                nosy_match_id, match_datetime, league, team1, team2,
                ms1, ms0, ms2, alt25, ust25,
                created_at, updated_at
            )
            VALUES(
                :nosy_match_id, :match_datetime, :league, :team1, :team2,
                :ms1, :ms0, :ms2, :alt25, :ust25,
                :created_at, :updated_at
            )
            ON CONFLICT (nosy_match_id)
            DO UPDATE SET
                match_datetime = EXCLUDED.match_datetime,
                league         = EXCLUDED.league,
                team1          = EXCLUDED.team1,
                team2          = EXCLUDED.team2,
                ms1            = EXCLUDED.ms1,
                ms0            = EXCLUDED.ms0,
                ms2            = EXCLUDED.ms2,
                alt25          = EXCLUDED.alt25,
                ust25          = EXCLUDED.ust25,
                updated_at     = EXCLUDED.updated_at
        """),
        {
            "nosy_match_id": nosy_match_id,
            "match_datetime": match_datetime,
            "league": league,
            "team1": team1,
            "team2": team2,
            "ms1": ms1,
            "ms0": ms0,
            "ms2": ms2,
            "alt25": alt25,
            "ust25": ust25,
            "created_at": now,
            "updated_at": now,
        },
    )
    return True

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="MatchMotor API", version="0.4.0")

@app.on_event("startup")
def startup():
    ensure_schema()

@app.get("/health")
def health():
    return {"ok": True, "time": dt.datetime.utcnow().isoformat()}

# --------------------
# Matches (filtrelenenler)
# --------------------
@app.get("/matches")
def list_matches(limit: int = 50):
    limit = max(1, min(500, limit))
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    id,
                    nosy_match_id,
                    datetime,
                    league,
                    team1,
                    team2,
                    ms1, ms0, ms2,
                    alt25, ust25
                FROM matches
                ORDER BY match_datetime DESC NULLS LAST
                LIMIT :limit
            """),
            {"limit": limit},
        ).mappings().all()
    return {"count": len(rows), "data": list(rows)}

# Havuz (Nosy matches)
@app.get("/nosy-matches")
def list_nosy_matches(limit: int = 50):
    limit = max(1, min(500, limit))
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    nosy_match_id,
                    match_datetime,
                    league,
                    team1,
                    team2
                FROM nosy_matches
                ORDER BY match_datetime DESC NULLS LAST
                LIMIT :limit
            """),
            {"limit": limit},
        ).mappings().all()
    return {"count": len(rows), "data": list(rows)}

@app.post("/admin/clear")
def clear_all():
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM match_odds;"))
        conn.execute(text("DELETE FROM match_results;"))
        conn.execute(text("DELETE FROM matches;"))
        conn.execute(text("DELETE FROM nosy_matches;"))
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

            conn.execute(
                text("""
                    INSERT INTO nosy_matches(
                        nosy_match_id, date, time, match_datetime, league, country, team1, team2, raw_json
                    )
                    VALUES(:mid, :date, :time, :dt, :league, :country, :team1, :team2, :raw_json)
                    ON CONFLICT(nosy_match_id) DO UPDATE SET
                        date=excluded.date,
                        time=excluded.time,
                        match_datetime=excluded.match_datetime,
                        league=excluded.league,
                        country=excluded.country,
                        team1=excluded.team1,
                        team2=excluded.team2,
                        raw_json=excluded.raw_json
                """),
                {
                    "mid": match_id,
                    "date": str(date_val),
                    "time": str(time_val),
                    "dt": str(dt_val),
                    "league": str(league),
                    "country": str(country),
                    "team1": str(team1),
                    "team2": str(team2),
                    "raw_json": _dump_json(item),
                },
            )
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
    raw = _dump_json(payload)

    with engine.begin() as conn:
        try:
            conn.execute(
                text("""
                    INSERT INTO match_odds (nosy_match_id, fetched_at, raw_json, payload)
                    VALUES (:mid, :fetched_at, :raw_json, :payload)
                    ON CONFLICT (nosy_match_id)
                    DO UPDATE SET
                        fetched_at = EXCLUDED.fetched_at,
                        raw_json   = EXCLUDED.raw_json,
                        payload    = EXCLUDED.payload
                """),
                {"mid": match_id, "fetched_at": now, "raw_json": raw, "payload": raw},
            )

            # ✅ Otomatik filtre -> matches tablosuna ekle (şartları sağlıyorsa)
            promoted = _upsert_filtered_match_from_odds(conn, match_id, payload)

        except Exception as e:
            raise HTTPException(status_code=500, detail={"where": "db_upsert_match_odds", "error": str(e)})

    return {"ok": True, "match_id": match_id, "saved": True, "promoted_to_matches": promoted, "nosy": payload}

@app.get("/opening-odds-exists-nosy")
def opening_odds_exists_nosy(match_id: int = Query(..., description="Nosy MatchID")):
    payload = nosy_call(
        "bettable-matches/opening-odds",
        params={"matchID": match_id},
        api_kind="odds",
    )
    data = payload.get("data") or []
    exists = (payload.get("status") == "success") and (len(data) > 0)
    return {
        "match_id": match_id,
        "exists_in_nosy": exists,
        "status": payload.get("status"),
        "rowCount": payload.get("rowCount"),
        "endpoint": payload.get("endpoint"),
    }

@app.get("/nosy-results")
def nosy_results(match_id: int = Query(..., description="Nosy MatchID")):
    return nosy_call("bettable-result", params={"matchID": match_id}, api_kind="results")

@app.get("/nosy-result-details")
def nosy_result_details(match_id: int = Query(..., description="Nosy MatchID")):
    payload = nosy_call("bettable-result/details", params={"matchID": match_id}, api_kind="results")

    now = dt.datetime.utcnow().isoformat()
    raw = _dump_json(payload)

    with engine.begin() as conn:
        try:
            conn.execute(
                text("""
                    INSERT INTO match_results(nosy_match_id, fetched_at, raw_json, payload)
                    VALUES(:mid, :fetched_at, :raw_json, :payload)
                    ON CONFLICT (nosy_match_id)
                    DO UPDATE SET
                        fetched_at = EXCLUDED.fetched_at,
                        raw_json   = EXCLUDED.raw_json,
                        payload    = EXCLUDED.payload
                """),
                {"mid": match_id, "fetched_at": now, "raw_json": raw, "payload": raw},
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail={"where": "db_upsert_match_results", "error": str(e)})

    return payload
    
