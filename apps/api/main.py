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
    """
    DB şemalarını oluşturur ve eski/legacy kolonları yeni düzene migrasyonla uyumlar.
    """
    dialect = engine.dialect.name
    is_sqlite = dialect.startswith("sqlite")

    def _get_cols(conn, table: str) -> set[str]:
        if is_sqlite:
            rows = conn.execute(text(f"PRAGMA table_info({table});")).mappings().all()
            return {r["name"] for r in rows}
        rows = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :t
        """), {"t": table}).all()
        return {r[0] for r in rows}

    def _add_col(conn, table: str, col: str, col_type_sql: str) -> None:
        if col in _get_cols(conn, table):
            return
        if is_sqlite:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type_sql};"))
        else:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type_sql};"))

    with engine.begin() as conn:
        # --------- Tables (minimum columns used by code) --------------------
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS nosy_matches (
            id            INTEGER PRIMARY KEY,
            nosy_match_id BIGINT UNIQUE,
            match_datetime TEXT,
            date          TEXT,
            time          TEXT,
            league        TEXT,
            country       TEXT,
            team1         TEXT,
            team2         TEXT,
            fetched_at    TEXT,
            raw_json      TEXT,
            payload       TEXT
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS matches (
            id            INTEGER PRIMARY KEY,
            nosy_match_id BIGINT UNIQUE,
            match_datetime TEXT,
            league        TEXT,
            team1         TEXT,
            team2         TEXT,
            ms1           DOUBLE PRECISION,
            ms0           DOUBLE PRECISION,
            ms2           DOUBLE PRECISION,
            alt25         DOUBLE PRECISION,
            ust25         DOUBLE PRECISION,
            created_at    TEXT,
            updated_at    TEXT
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS match_odds (
            id         INTEGER PRIMARY KEY,
            match_id   BIGINT UNIQUE,
            fetched_at TEXT,
            raw_json   TEXT,
            payload    TEXT
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS match_results (
            id         INTEGER PRIMARY KEY,
            match_id   BIGINT UNIQUE,
            fetched_at TEXT,
            raw_json   TEXT,
            payload    TEXT
        );
        """))

        # --------- Legacy migrations / column sync --------------------------
        # matches legacy: (match_id, datetime, ...)
        cols_matches = _get_cols(conn, "matches")

        if "nosy_match_id" not in cols_matches and "match_id" in cols_matches:
            _add_col(conn, "matches", "nosy_match_id", "BIGINT")
            conn.execute(text("UPDATE matches SET nosy_match_id = match_id WHERE nosy_match_id IS NULL;"))

        if "match_datetime" not in cols_matches and "datetime" in cols_matches:
            _add_col(conn, "matches", "match_datetime", "TEXT")
            conn.execute(text("UPDATE matches SET match_datetime = datetime WHERE match_datetime IS NULL;"))

        # ensure all expected cols exist (safe add)
        for col, typ in [
            ("nosy_match_id", "BIGINT"),
            ("match_datetime", "TEXT"),
            ("league", "TEXT"),
            ("team1", "TEXT"),
            ("team2", "TEXT"),
            ("ms1", "DOUBLE PRECISION"),
            ("ms0", "DOUBLE PRECISION"),
            ("ms2", "DOUBLE PRECISION"),
            ("alt25", "DOUBLE PRECISION"),
            ("ust25", "DOUBLE PRECISION"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ]:
            _add_col(conn, "matches", col, typ)

        # nosy_matches legacy
        cols_nosy = _get_cols(conn, "nosy_matches")

        if "nosy_match_id" not in cols_nosy and "match_id" in cols_nosy:
            _add_col(conn, "nosy_matches", "nosy_match_id", "BIGINT")
            conn.execute(text("UPDATE nosy_matches SET nosy_match_id = match_id WHERE nosy_match_id IS NULL;"))

        if "match_datetime" not in cols_nosy and "datetime" in cols_nosy:
            _add_col(conn, "nosy_matches", "match_datetime", "TEXT")
            conn.execute(text("UPDATE nosy_matches SET match_datetime = datetime WHERE match_datetime IS NULL;"))

        for col, typ in [
            ("nosy_match_id", "BIGINT"),
            ("match_datetime", "TEXT"),
            ("date", "TEXT"),
            ("time", "TEXT"),
            ("league", "TEXT"),
            ("country", "TEXT"),
            ("team1", "TEXT"),
            ("team2", "TEXT"),
            ("fetched_at", "TEXT"),
            ("raw_json", "TEXT"),
            ("payload", "TEXT"),
        ]:
            _add_col(conn, "nosy_matches", col, typ)

        # match_odds / match_results legacy
        for table in ["match_odds", "match_results"]:
            for col, typ in [("payload", "TEXT"), ("raw_json", "TEXT"), ("fetched_at", "TEXT")]:
                _add_col(conn, table, col, typ)

        # --------- Indexes (only if column exists) --------------------------
        cols_matches = _get_cols(conn, "matches")
        cols_nosy = _get_cols(conn, "nosy_matches")

        idx_statements: list[str] = []
        if "match_datetime" in cols_matches:
            idx_statements.append("CREATE INDEX IF NOT EXISTS idx_matches_dt ON matches(match_datetime);")
        if "nosy_match_id" in cols_matches:
            idx_statements.append("CREATE INDEX IF NOT EXISTS idx_matches_nosy_id ON matches(nosy_match_id);")
        if "match_datetime" in cols_nosy:
            idx_statements.append("CREATE INDEX IF NOT EXISTS idx_nosy_matches_dt ON nosy_matches(match_datetime);")
        if "nosy_match_id" in cols_nosy:
            idx_statements.append("CREATE INDEX IF NOT EXISTS idx_nosy_matches_nosy_id ON nosy_matches(nosy_match_id);")

        for stmt in idx_statements:
            try:
                conn.execute(text(stmt))
            except Exception:
                # index hatası uygulamayı düşürmesin (özellikle eski kolon isimleri yüzünden)
                pass

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
    """
    Filtreyi geçen maçlar (matches tablosu)
    """
    limit = max(1, min(500, limit))
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
                id,
                nosy_match_id,
                match_datetime,
                league,
                team1,
                team2,
                ms1, ms0, ms2,
                alt25, ust25,
                created_at,
                updated_at
            FROM matches
            ORDER BY match_datetime DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
    return {"count": len(rows), "data": list(rows)}


@app.get("/nosy-matches")
def list_nosy_matches(limit: int = 50):
    """
    Nosy'den gelen ham maçlar (nosy_matches tablosu)
    """
    limit = max(1, min(500, limit))
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
                id,
                nosy_match_id,
                match_datetime,
                date,
                time,
                league,
                country,
                team1,
                team2,
                fetched_at
            FROM nosy_matches
            ORDER BY match_datetime DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
    return {"count": len(rows), "data": list(rows)}@app.get("/nosy-matches")
def list_nosy_matches(limit: int = 50):
    """
    Nosy'den gelen ham maçlar (nosy_matches tablosu)
    """
    limit = max(1, min(500, limit))
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
                id,
                nosy_match_id,
                match_datetime,
                league,
                country,
                team1,
                team2,
                home_win, draw, away_win,
                under25, over25,
                betcount,
                fetched_at
            FROM nosy_matches
            ORDER BY match_datetime DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
    return {"count": len(rows), "data": list(rows)}@app.get("/nosy-matches")
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

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO match_odds (match_id, fetched_at, raw_json, payload)
            VALUES (:match_id, :fetched_at, :raw_json, :payload)
            ON CONFLICT (match_id)
            DO UPDATE SET
                fetched_at = EXCLUDED.fetched_at,
                raw_json   = EXCLUDED.raw_json,
                payload    = EXCLUDED.payload
        """), {
            "match_id": match_id,
            "fetched_at": now,
            "raw_json": _dump_json(payload),
            "payload": _dump_json(payload),
        })

    return {
        "ok": True,
        "match_id": match_id,
        "saved": True
    }
