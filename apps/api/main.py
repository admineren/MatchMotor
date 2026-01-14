import os
import json
import requests

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple
from collections import Counter

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text

# ==========================================================
# CONFIG
# ==========================================================
TR_TZ = ZoneInfo("Europe/Istanbul")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
# IMPORTANT:
# - Neon connection strings are usually already in the correct form.
# - If you use a Postgres URL, prefer: postgresql+psycopg://...
#   (psycopg3). If you use psycopg2 then: postgresql+psycopg2://...
engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()

FLASHSCORE_BASE_URL = os.getenv(
    "FLASHSCORE_BASE_URL",
    "https://flashscore4.p.rapidapi.com/api/flashscore/v1"
).strip().rstrip("/")

FLASHSCORE_RAPIDAPI_HOST = os.getenv(
    "FLASHSCORE_RAPIDAPI_HOST",
    "flashscore4.p.rapidapi.com"
).strip()

# Flashscore matches endpoint path (in case RapidAPI changes / you use a different provider)
# Default guess: football/matches/{YYYY-MM-DD}
FLASHSCORE_MATCHES_PATH_TEMPLATE = os.getenv(
    "FLASHSCORE_MATCHES_PATH_TEMPLATE",
    "match/list/1/{date}"
).strip().lstrip("/")

# ==========================================================
# HELPERS
# ==========================================================
def _require_db():
    if engine is None:
        raise HTTPException(status_code=500, detail="DATABASE_URL env eksik veya engine kurulamadı.")

def _require_rapidapi_key():
    if not RAPIDAPI_KEY:
        raise HTTPException(status_code=500, detail="RAPIDAPI_KEY env eksik.")

def _dump_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def flashscore_get(path: str, *, params: Optional[dict] = None) -> dict:
    """
    Flashscore RapidAPI GET helper.
    path örn: 'general/1/countries'
    """
    _require_rapidapi_key()

    url = f"{FLASHSCORE_BASE_URL}/{path.lstrip('/')}"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": FLASHSCORE_RAPIDAPI_HOST,
    }

    try:
        r = requests.get(url, headers=headers, params=(params or {}), timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Flashscore bağlantı hatası: {e}")

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

def classify_stage(stage: Optional[str]) -> str:
    # Normalize "Finished", "Live", "Postponed", etc.
    s = (stage or "").strip()
    if not s:
        return "Unknown"
    # Keep original if it looks meaningful, else Title-case
    return s[:1].upper() + s[1:]

def _fs_ts_to_tr(ts: Any) -> Optional[datetime]:
    """
    Flashscore timestamps are commonly epoch seconds.
    Return Turkey-aware datetime.
    """
    if ts is None:
        return None
    try:
        ts_i = int(str(ts).strip())
    except Exception:
        return None
    return datetime.fromtimestamp(ts_i, tz=timezone.utc).astimezone(TR_TZ)

def _fs_is_finished(match_obj: dict) -> bool:
    """
    Decide if a match is finished.
    We primarily use stage / status fields if available.
    """
    stage = (
        match_obj.get("stage")
        or match_obj.get("status")
        or match_obj.get("stageName")
        or match_obj.get("eventStage")
        or ""
    )
    s = str(stage).strip().lower()
    return s == "finished" or s == "ft" or s == "ended"

def _fs_extract_score(match_obj: dict) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract full-time score if present.
    Tries common shapes:
      - match['result']['home'], match['result']['away']
      - match['homeScore'], match['awayScore']
      - match['score']['home'], match['score']['away']
      - match['result']['ft']['home'], match['result']['ft']['away']
    """
    def to_int(x):
        try:
            if x is None:
                return None
            s = str(x).strip()
            if s == "" or s == "-":
                return None
            return int(float(s))
        except Exception:
            return None

    # flat
    ft_home = to_int(match_obj.get("homeScore"))
    ft_away = to_int(match_obj.get("awayScore"))
    if ft_home is not None and ft_away is not None:
        return ft_home, ft_away

    # score dict
    sc = match_obj.get("score") or {}
    if isinstance(sc, dict):
        ft_home = to_int(sc.get("home"))
        ft_away = to_int(sc.get("away"))
        if ft_home is not None and ft_away is not None:
            return ft_home, ft_away

    # result dict
    res = match_obj.get("result") or {}
    if isinstance(res, dict):
        # direct
        ft_home = to_int(res.get("home"))
        ft_away = to_int(res.get("away"))
        if ft_home is not None and ft_away is not None:
            return ft_home, ft_away
        # nested ft
        ft = res.get("ft") or {}
        if isinstance(ft, dict):
            ft_home = to_int(ft.get("home"))
            ft_away = to_int(ft.get("away"))
            if ft_home is not None and ft_away is not None:
                return ft_home, ft_away

    return None, None

def _fs_pick_ms_odds(match_obj: dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Pull 1X2 odds if present in the match payload.
    (Some providers include it; if not, keep None.)
    """
    def to_float(x):
        try:
            if x is None:
                return None
            s = str(x).strip()
            if s == "" or s == "-":
                return None
            return float(s)
        except Exception:
            return None

    odds = match_obj.get("odds") or match_obj.get("1x2") or match_obj.get("ms") or {}
    if isinstance(odds, dict):
        ms1 = to_float(odds.get("home") or odds.get("1") or odds.get("ms1"))
        ms0 = to_float(odds.get("draw") or odds.get("x") or odds.get("ms0"))
        ms2 = to_float(odds.get("away") or odds.get("2") or odds.get("ms2"))
        if ms1 is not None or ms0 is not None or ms2 is not None:
            return ms1, ms0, ms2

    # Sometimes odds are under bookmakers[0]['markets'][...]
    return None, None, None

# ==========================================================
# DB SCHEMA
# ==========================================================
def ensure_schema():
    _require_db()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS flash_finished_ms (
                id BIGSERIAL PRIMARY KEY,
                flash_match_id TEXT NOT NULL UNIQUE,

                match_datetime_tr TEXT,
                date TEXT,
                time TEXT,

                country_name TEXT,
                tournament_name TEXT,

                home TEXT,
                away TEXT,

                ft_home INT,
                ft_away INT,

                ms1 DOUBLE PRECISION,
                ms0 DOUBLE PRECISION,
                ms2 DOUBLE PRECISION,

                fetched_at_tr TEXT,
                raw_json TEXT,

                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """))
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_flash_finished_ms_date ON flash_finished_ms(date);"""))
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_flash_finished_ms_fetched ON flash_finished_ms(fetched_at_tr);"""))

# ==========================================================
# APP
# ==========================================================
app = FastAPI(
    title="MatchMotor API (Flashscore-only)",
    version="1.0.0",
    description="Flashscore (RapidAPI) -> Postgres (Neon) | Only finished matches (1X2 if available).",
)

@app.on_event("startup")
def _startup():
    if engine is not None:
        ensure_schema()

@app.get("/health")
def health():
    now_utc = datetime.now(timezone.utc)
    now_tr = now_utc.astimezone(TR_TZ)
    return {
        "ok": True,
        "time_utc": now_utc.isoformat(),
        "time_tr": now_tr.isoformat(),
        "tz": "Europe/Istanbul",
        "db": {"connected": bool(engine), "url_set": bool(DATABASE_URL)},
        "flashscore": {
            "base_url": FLASHSCORE_BASE_URL,
            "host": FLASHSCORE_RAPIDAPI_HOST,
            "rapidapi_key_set": bool(RAPIDAPI_KEY),
            "matches_path_template": FLASHSCORE_MATCHES_PATH_TEMPLATE,
        },
    }

@app.get("/flashscore/check/base", tags=["Flashscore"])
def flashscore_check_base():
    """
    Flashscore base URL'ye GET atar (sonunda /ping yok).
    Rate-limit header'larını göstermek için.
    """
    _require_rapidapi_key()
    url = f"{FLASHSCORE_BASE_URL}/match/list/1/{datetime.now(TR_TZ).date().isoformat()}"

    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": FLASHSCORE_RAPIDAPI_HOST,
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Flashscore bağlantı hatası: {e}")

    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise HTTPException(status_code=r.status_code, detail={"url": url, "body": body})

    h = {k.lower(): v for k, v in r.headers.items()}
    limit = h.get("x-ratelimit-requests-limit")
    remaining = h.get("x-ratelimit-requests-remaining")
    reset_raw = h.get("x-ratelimit-requests-reset")

    now_tr = datetime.now(TR_TZ)

    reset_tr_iso = None
    seconds_until_reset = None
    if reset_raw and str(reset_raw).isdigit():
        reset_epoch = int(reset_raw)
        reset_dt_tr = datetime.fromtimestamp(reset_epoch, tz=timezone.utc).astimezone(TR_TZ)
        reset_tr_iso = reset_dt_tr.isoformat()
        seconds_until_reset = max(0, int((reset_dt_tr - now_tr).total_seconds()))

    return {
        "ok": True,
        "flashscore": {
            "status_code": r.status_code,
            "host": FLASHSCORE_RAPIDAPI_HOST,
            "base_url": FLASHSCORE_BASE_URL,
            "rate_limits": {
                "requests_limit": limit,
                "requests_remaining": remaining,
                "requests_reset_raw": reset_raw,
                "requests_reset_tr": reset_tr_iso,
                "seconds_until_reset": seconds_until_reset,
            },
            "turkey_time": now_tr.isoformat(),
        },
    }

@app.get("/flashscore/matches/{date}", tags=["Flashscore"])
def flashscore_matches(date: str):
    """
    Raw matches of a date from Flashscore (RapidAPI).
    Default endpoint guess: football/matches/{date}
    You can override with FLASHSCORE_MATCHES_PATH_TEMPLATE env.
    """
    path = FLASHSCORE_MATCHES_PATH_TEMPLATE.format(date=date)
    return flashscore_get(path)

@app.post("/flashscore/db/finished-ms/sync-date", tags=["Flashscore DB"])
def flashscore_db_finished_ms_sync_date(
    date: str = Query(..., description="YYYY-MM-DD"),
    limit_write: int = Query(5000, ge=1, le=20000, description="DB yazım limit (güvenlik)"),
    sample: int = Query(3, ge=0, le=20, description="response'a örnek maç koy (debug)"),
):
    """
    1) /flashscore/matches/{date} ile maçları çek
    2) sadece Finished olanları flash_finished_ms tablosuna INSERT (conflict DO NOTHING)
    """
    ensure_schema()

    # DB before count
    with engine.begin() as conn:
        db_count_before = conn.execute(
            text("SELECT COUNT(*)::int FROM flash_finished_ms WHERE date = :d"),
            {"d": date},
        ).scalar() or 0

    fetched_at_tr = datetime.now(TR_TZ).isoformat()

    payload = flashscore_matches(date)  # same process in-app (no extra config)
    data = payload.get("data") if isinstance(payload, dict) else None
    if data is None:
        # Some providers return list directly
        data = payload if isinstance(payload, list) else []

    if not isinstance(data, list):
        # Fallback: try payload["data"]["events"] etc.
        if isinstance(payload, dict):
            for k in ("events", "matches", "items"):
                v = payload.get(k)
                if isinstance(v, list):
                    data = v
                    break
        if not isinstance(data, list):
            data = []

    api_received_total = len(data)

    # Stage stats
    api_stage_counts = Counter()
    examples = []
    inserted_new = 0
    eligible_for_db = 0
    limited = False

    # Date distribution based on timestamp->TR date
    eligible_date_dist = Counter()
    inserted_date_dist = Counter()
    min_match_dt_tr = None
    max_match_dt_tr = None

    skipped = {
        "non_dict": 0,
        "missing_id": 0,
        "missing_timestamp": 0,
        "not_finished": 0,
        "no_score": 0,
    }
    skipped_stage = Counter()

    with engine.begin() as conn:
        for m in data:
            if not isinstance(m, dict):
                skipped["non_dict"] += 1
                continue

            stage_raw = (
                m.get("stage")
                or m.get("status")
                or m.get("stageName")
                or m.get("eventStage")
                or "Unknown"
            )
            stage = classify_stage(str(stage_raw))
            api_stage_counts[stage] += 1

            if not _fs_is_finished(m):
                skipped["not_finished"] += 1
                skipped_stage[stage] += 1
                continue

            # Basic identifiers
            mid = m.get("id") or m.get("eventId") or m.get("matchId")
            if mid is None:
                skipped["missing_id"] += 1
                continue
            mid_s = str(mid)

            ts = m.get("timestamp") or m.get("startTimestamp") or m.get("startTime") or m.get("start_time")
            dt_tr = _fs_ts_to_tr(ts)
            if dt_tr is None:
                skipped["missing_timestamp"] += 1
                continue

            ft_home, ft_away = _fs_extract_score(m)
            if ft_home is None or ft_away is None:
                skipped["no_score"] += 1
                continue

            # Country + tournament names (best effort)
            country_name = (
                (m.get("country") or {}).get("name")
                if isinstance(m.get("country"), dict)
                else m.get("countryName") or m.get("country") or None
            )
            tournament_name = (
                (m.get("tournament") or {}).get("name")
                if isinstance(m.get("tournament"), dict)
                else m.get("tournamentName") or m.get("tournament") or None
            )

            # Teams
            ht = m.get("homeTeam") or m.get("home") or {}
            at = m.get("awayTeam") or m.get("away") or {}
            home_name = ht.get("name") if isinstance(ht, dict) else (m.get("homeName") or str(ht))
            away_name = at.get("name") if isinstance(at, dict) else (m.get("awayName") or str(at))

            ms1, ms0, ms2 = _fs_pick_ms_odds(m)

            eligible_for_db += 1
            eligible_date_dist[dt_tr.date().isoformat()] += 1
            if min_match_dt_tr is None or dt_tr < min_match_dt_tr:
                min_match_dt_tr = dt_tr
            if max_match_dt_tr is None or dt_tr > max_match_dt_tr:
                max_match_dt_tr = dt_tr
            if inserted_new >= limit_write:
                limited = True
                break

            res = conn.execute(
                text("""
                    INSERT INTO flash_finished_ms(
                        flash_match_id,
                        match_datetime_tr,
                        date, time,
                        country_name,
                        tournament_name,
                        home, away,
                        ft_home, ft_away,
                        ms1, ms0, ms2,
                        fetched_at_tr,
                        raw_json,
                        updated_at
                    )
                    VALUES(
                        :flash_match_id,
                        :match_datetime_tr,
                        :date, :time,
                        :country_name,
                        :tournament_name,
                        :home, :away,
                        :ft_home, :ft_away,
                        :ms1, :ms0, :ms2,
                        :fetched_at_tr,
                        :raw_json,
                        NOW()
                    )
                    ON CONFLICT(flash_match_id) DO NOTHING
                """),
                {
                    "flash_match_id": mid_s,
                    "match_datetime_tr": dt_tr.isoformat(),
                    "date": dt_tr.date().isoformat(),
                    "time": dt_tr.time().strftime("%H:%M:%S"),
                    "country_name": country_name,
                    "tournament_name": tournament_name,
                    "home": home_name,
                    "away": away_name,
                    "ft_home": ft_home,
                    "ft_away": ft_away,
                    "ms1": ms1,
                    "ms0": ms0,
                    "ms2": ms2,
                    "fetched_at_tr": fetched_at_tr,
                    "raw_json": _dump_json(m),
                },
            )

            # SQLAlchemy rowcount: INSERT olduysa genelde 1, conflict DO NOTHING ise 0
            if getattr(res, "rowcount", 0) == 1:
                inserted_new += 1
                inserted_date_dist[dt_tr.date().isoformat()] += 1

            if sample > 0 and len(examples) < sample:
                examples.append({
                    "flash_match_id": mid_s,
                    "stage": stage,
                    "match_datetime_tr": dt_tr.isoformat(),
                    "home": home_name,
                    "away": away_name,
                    "ft": f"{ft_home}-{ft_away}",
                    "ms1": ms1, "ms0": ms0, "ms2": ms2,
                })

        db_count_after = conn.execute(
            text("SELECT COUNT(*)::int FROM flash_finished_ms WHERE date = :d"),
            {"d": date},
        ).scalar() or 0

    # "Finished" key'i bazen farklı case gelebilir: normalize toplamı da veriyoruz
    api_finished_total_normalized = sum(
        v for k, v in api_stage_counts.items() if (k or "").strip().lower() == "finished"
    )

    resp = {
        "ok": True,
        "request_date": date,
        "api_received_total": api_received_total,
        "api_stage_counts": dict(api_stage_counts),
        "api_finished_total_normalized": api_finished_total_normalized,
        "eligible_for_db": eligible_for_db,
        "eligible_date_distribution_tr": dict(eligible_date_dist),
        "inserted_new": inserted_new,
        "inserted_date_distribution_tr": dict(inserted_date_dist),
        "match_datetime_tr_range": {
            "min": min_match_dt_tr.isoformat() if min_match_dt_tr else None,
            "max": max_match_dt_tr.isoformat() if max_match_dt_tr else None,
        },
        "limited": limited,
        "skipped": {
            **skipped,
            "by_non_finished_stage": dict(skipped_stage),
        },
        "db": {
            "count_before": db_count_before,
            "count_after": db_count_after,
            "delta": db_count_after - db_count_before,
        },
        "limit_write": limit_write,
        "fetched_at_tr": fetched_at_tr,
    }
    if sample > 0:
        resp["examples"] = examples
    return resp

@app.get("/flashscore/db/finished-ms", tags=["Flashscore DB"])
def flashscore_db_finished_ms(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    country: Optional[str] = Query(None, description="Örn: Brazil"),
    tournament: Optional[str] = Query(None, description="Örn: BRAZIL: Copinha"),
    limit: int = Query(500, ge=1, le=5000),
):
    ensure_schema()

    where = []
    params: Dict[str, Any] = {"limit": limit}

    if date:
        where.append("date = :date")
        params["date"] = date
    if country:
        where.append("country_name ILIKE :country")
        params["country"] = f"%{country}%"
    if tournament:
        where.append("tournament_name ILIKE :tournament")
        params["tournament"] = f"%{tournament}%"

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = text(f"""
        SELECT
            flash_match_id,
            match_datetime_tr,
            date, time,
            country_name,
            tournament_name,
            home, away,
            ft_home, ft_away,
            ms1, ms0, ms2,
            fetched_at_tr,
            updated_at
        FROM flash_finished_ms
        {where_sql}
        ORDER BY date DESC, time DESC
        LIMIT :limit
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return {
        "ok": True,
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }

@app.get("/flashscore/db/finished-ms/daily-counts", tags=["Flashscore DB"])
def flashscore_db_finished_ms_daily_counts():
    if engine is None:
        raise HTTPException(status_code=500, detail="DATABASE_URL/engine yok")

    sql = text("""
        SELECT
            date,
            COUNT(*) AS match_count
        FROM flash_finished_ms
        GROUP BY date
        ORDER BY date
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()

    return {
        "ok": True,
        "items": [
            {"date": r.date, "count": r.match_count}
            for r in rows
        ]
    }

@app.get("/flashscore/db/finished-ms/by-tournament", tags=["Flashscore DB"])
def flashscore_db_finished_ms_by_tournament(
    limit: int = Query(200, ge=1, le=2000),
    include_country: int = Query(1, ge=0, le=1, description="1=country+tournament, 0=sadece tournament")
):
    ensure_schema()
    if engine is None:
        raise HTTPException(status_code=500, detail="DATABASE_URL/engine yok")

    if include_country == 1:
        sql = text("""
            SELECT
                COALESCE(country_name, '') AS country_name,
                COALESCE(tournament_name, '') AS tournament_name,
                COUNT(*)::int AS match_count
            FROM flash_finished_ms
            GROUP BY 1, 2
            ORDER BY match_count DESC
            LIMIT :limit
        """)
    else:
        sql = text("""
            SELECT
                COALESCE(tournament_name, '') AS tournament_name,
                COUNT(*)::int AS match_count
            FROM flash_finished_ms
            GROUP BY 1
            ORDER BY match_count DESC
            LIMIT :limit
        """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit}).mappings().all()

    # JSON formatını temiz döndürelim
    if include_country == 1:
        items = [
            {
                "country_name": r["country_name"] or None,
                "tournament_name": r["tournament_name"] or None,
                "match_count": int(r["match_count"])
            }
            for r in rows
        ]
    else:
        items = [
            {
                "tournament_name": r["tournament_name"] or None,
                "match_count": int(r["match_count"])
            }
            for r in rows
        ]

    return {"ok": True, "count": len(items), "items": items}


