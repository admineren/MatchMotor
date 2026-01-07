import os
import requests
import json
import math
import datetime as dt

from fastapi import FastAPI, HTTPException, Query, APIRouter, Path
from fastapi.responses import JSONResponse


from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text

# ==========================================================
# DATABASE
# ==========================================================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
) if DATABASE_URL else None


# ---------------------------
# Config (ENV)
# ---------------------------
NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()

# Tüm veri endpointleri buradan çağrılır (service zorunlu)
NOSY_SERVICE_BASE_URL = os.getenv(
    "NOSY_SERVICE_BASE_URL",
    "https://www.nosyapi.com/apiv2/service"
).strip().rstrip("/")

# Sadece check endpointi buradan çağrılır (service içermez)
NOSY_CHECK_BASE_URL = os.getenv(
    "NOSY_CHECK_BASE_URL",
    "https://www.nosyapi.com/apiv2"
).strip().rstrip("/")

# Check için API ID'ler (zorunlu değil; sadece check endpointlerini açacaksan gerekli)
NOSY_CHECK_API_ID_ODDS = os.getenv("NOSY_CHECK_API_ID_ODDS", "").strip()
NOSY_CHECK_API_ID_BETTABLE_RESULT = os.getenv("NOSY_CHECK_API_ID_BETTABLE_RESULT", "").strip()
NOSY_CHECK_API_ID_MATCHES_RESULT = os.getenv("NOSY_CHECK_API_ID_MATCHES_RESULT", "").strip()

# ---------------------------
# Timezone (Türkiye saati)
# ---------------------------
try:
    from zoneinfo import ZoneInfo  # Py3.9+
    TR_TZ = ZoneInfo("Europe/Istanbul")
except Exception:
    TR_TZ = None  # zoneinfo yoksa health'ta sadece UTC döneceğiz

# ---------------------------
# Helpers
# ---------------------------
def _dump_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def _to_int_score(x):
    """Skor/kart/korner gibi alanlarda: '-' boş/null => None, '0' => 0"""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s == "-" or s.lower() == "null":
        return None
    try:
        return int(s)
    except Exception:
        return None

def _meta_map(match_result_list) -> dict:
    m = {}
    if isinstance(match_result_list, list):
        for it in match_result_list:
            if not isinstance(it, dict):
                continue
            k = it.get("metaName")
            v = it.get("value")
            if k is not None:
                m[str(k)] = v
    return m

def _meta_ci_get(match_result, wanted_key: str):
    if not match_result:
        return None
    w = wanted_key.lower()

    if isinstance(match_result, dict):
        for k, v in match_result.items():
            if isinstance(k, str) and k.lower() == w:
                if isinstance(v, dict) and "value" in v:
                    return v.get("value")
                return v
        return None

    if isinstance(match_result, list):
        for row in match_result:
            if not isinstance(row, dict):
                continue
            name = row.get("metaName") or row.get("MetaName") or row.get("metaname")
            if isinstance(name, str) and name.lower() == w:
                return row.get("value") or row.get("Value")
    return None

def _pick_int_from_match_result(match_result, *keys):
    """
    keys: ("msHomeScore", "mshomescore", ...)
    döner: (int|None, used_key|None, raw_value)
    """
    for k in keys:
        raw = _meta_ci_get(match_result, k)
        iv = _to_int_score(raw)
        if iv is not None:
            return iv, k, raw
    return None, None, None

def _debug_no_score(reason: str, mid: int, item: dict, keys_tried: dict):
    mr = item.get("matchResult") or []
    sample = mr[:6] if isinstance(mr, list) else list(mr.items())[:6] if isinstance(mr, dict) else []
    return {
        "match_id": mid,
        "reason": reason,
        "GameResult": item.get("GameResult"),
        "Result": item.get("Result"),
        "LiveStatus": item.get("LiveStatus"),
        "keys_tried": keys_tried,
        "matchResult_sample": sample,
    }

def _gr_int(item: dict):
        gr = item.get("GameResult")
        try:
            return int(str(gr).strip())
        except Exception:
            return None

def _take_details_data(details_payload):
        dd = (details_payload or {}).get("data")
        if isinstance(dd, list) and dd:
            return dd[0]
        if isinstance(dd, dict):
            return dd
        return None

def _require_api_key():
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env eksik.")

def _join_url(base: str, endpoint: str) -> str:
    base = (base or "").rstrip("/")
    endpoint = (endpoint or "").lstrip("/")
    return f"{base}/{endpoint}"

def nosy_service_call(endpoint: str, *, params: dict | None = None) -> dict:
    _require_api_key()
    url = _join_url(NOSY_SERVICE_BASE_URL, endpoint)

    q = dict(params or {})
    q["apiKey"] = NOSY_API_KEY

    try:
        r = requests.get(url, params=q, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Nosy bağlantı hatası: {e}")

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

def nosy_check_call(api_id: str) -> dict:
    _require_api_key()
    if not api_id:
        raise HTTPException(status_code=500, detail="Check için apiID env eksik.")

    url = _join_url(NOSY_CHECK_BASE_URL, "nosy-service/check")
    q = {"apiKey": NOSY_API_KEY, "apiID": api_id}

    try:
        r = requests.get(url, params=q, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Nosy check bağlantı hatası: {e}")

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

def make_aware(dt, tz):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt


# ==========================================================
# APP
# ==========================================================
app = FastAPI(
    title="MatchMotor API",
    version="0.1.0",
    description="NosyAPI proxy (DB yok, sadece altyapı ve test endpointleri).",
)

@app.on_event("startup")
def _startup():
    if engine is not None:
        ensure_schema()

@app.get("/health")
def health():
    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    now_tr = now_utc.astimezone(TR_TZ) if TR_TZ else None

    return {
        "ok": True,
        "time_utc": now_utc.isoformat(),
        "time_tr": now_tr.isoformat() if now_tr else None,
        "tz": "Europe/Istanbul" if TR_TZ else None,
        "nosy": {
            "service_base": NOSY_SERVICE_BASE_URL,
            "check_base": NOSY_CHECK_BASE_URL,
            "api_key_set": bool(NOSY_API_KEY),
            "check_ids_set": {
                "odds": bool(NOSY_CHECK_API_ID_ODDS),
                "bettable_result": bool(NOSY_CHECK_API_ID_BETTABLE_RESULT),
                "matches_result": bool(NOSY_CHECK_API_ID_MATCHES_RESULT),
            },
        },
    }


# ==========================================================
# DATABASE SCHEMA
# ==========================================================

def ensure_schema():
    if engine is None:
        raise RuntimeError("DATABASE_URL env eksik")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pool_matches (
                id BIGSERIAL PRIMARY KEY,
                nosy_match_id BIGINT NOT NULL UNIQUE,
                match_id BIGNIT,
                match_datetime TEXT,
                date TEXT,
                time TEXT,
                league TEXT,
                country TEXT,
                team1 TEXT,
                team2 TEXT,
                betcount INT,
                ms1 DOUBLE PRECISION,
                ms0 DOUBLE PRECISION,
                ms2 DOUBLE PRECISION,
                alt25 DOUBLE PRECISION,
                ust25 DOUBLE PRECISION,
                fetched_at_tr TEXT,
                raw_json TEXT
            );
        """))

        # 🔧 telefon kurtarıcı patch
        conn.execute(text("""ALTER TABLE pool_matches ADD COLUMN IF NOT EXISTS fetched_at_tr TEXT;"""))
        conn.execute(text("""ALTER TABLE pool_matches ADD COLUMN IF NOT EXISTS raw_json TEXT;"""))
        conn.execute(text("""ALTER TABLE pool_matches ADD COLUMN IF NOT EXISTS game_result INTEGER;"""))
        conn.execute(text("""ALTER TABLE pool_matches ADD COLUMN IF NOT EXISTS match_id BIGINT;"""))
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_pool_matches_match_id ON pool_matches(match_id);"""))

# -----------------------------
# FINISHED MATCHES (matches-result snapshot)
# -----------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finished_matches (
                id BIGSERIAL PRIMARY KEY,
                nosy_match_id BIGINT NOT NULL UNIQUE,
                match_id BIGNIT,

                match_datetime TEXT,
                date TEXT,
                time TEXT,

                league_code TEXT,
                league TEXT,
                country TEXT,
                team1 TEXT,
                team2 TEXT,

                betcount INT,
                ms1 DOUBLE PRECISION,
                ms0 DOUBLE PRECISION,
                ms2 DOUBLE PRECISION,
                alt25 DOUBLE PRECISION,
                ust25 DOUBLE PRECISION,

                ft_home INT,
                ft_away INT,
                ht_home INT,
                ht_away INT,

                mb INT,
                result INT,
                game_result INT,
                live_status INT,

                fetched_at_tr TEXT,
                raw_json TEXT,

                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """))

        conn.execute(text("""CREATE UNIQUE INDEX IF NOT EXISTS ux_finished_matches_mid ON finished_matches (nosy_match_id);"""))
        
        # finished_matches: corner + kart kolonları
        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS home_corner INTEGER"))
        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS away_corner INTEGER"))

        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS home_yellow INTEGER"))
        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS away_yellow INTEGER"))

        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS home_red INTEGER"))
        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS away_red INTEGER"))
        
        conn.execute(text("ALTER TABLE finished_matches ADD COLUMN IF NOT EXISTS match_id BIGINT"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_finished_matches_match_id ON finished_matches(match_id)"))

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

# ---------------------------
# Nosy CHECK endpoints (root base)
# ---------------------------

@app.get("/nosy/check/odds")
def nosy_check_odds():
    return nosy_check_call(NOSY_CHECK_API_ID_ODDS)

@app.get("/nosy/check/bettable-result")
def nosy_check_bettable_result():
    return nosy_check_call(NOSY_CHECK_API_ID_BETTABLE_RESULT)

@app.get("/nosy/check/matches-result")
def nosy_check_matches_result():
    return nosy_check_call(NOSY_CHECK_API_ID_MATCHES_RESULT)

# ---------------------------
# Nosy SERVICE proxy endpoints
# ---------------------------

@app.get("/nosy/bettable-matches")
def nosy_bettable_matches():
    # İddaa programını listeler
    return nosy_service_call("bettable-matches")

@app.get("/nosy/bettable-matches/date")
def nosy_bettable_matches_date():
    # Sistemde kayıtlı oyunların tarih bilgisini grup halinde döndürür (dokümandaki gibi)
    return nosy_service_call("bettable-matches/date")

@app.get("/nosy/bettable-matches/details")
def nosy_bettable_matches_details(matchID: int = Query(..., description="Nosy MatchID")):
    # İlgili maçın tüm market oranları (details)
    return nosy_service_call("bettable-matches/details", params={"matchID": matchID})

@app.get("/nosy/matches-result")
def nosy_matches_result():
    # Maç sonuçlarını toplu görüntülemek için
    return nosy_service_call("matches-result")

@app.get("/nosy/matches-result/details")
def nosy_matches_result_details(matchID: int = Query(..., description="Nosy MatchID")):
    # Tek maça ait maç sonucu
    return nosy_service_call("matches-result/details", params={"matchID": matchID})

@app.get("/nosy/bettable-result")
def nosy_bettable_result(matchID: int = Query(..., description="Nosy MatchID")):
    # İlgili maça ait oyun sonuçları (market sonuçları)
    return nosy_service_call("bettable-result", params={"matchID": matchID})

@app.get("/nosy/bettable-result/details")
def nosy_bettable_result_details(gameID: int = Query(..., description="Nosy gameID")):
    # Tekil oyun sonucu (game bazlı)
    return nosy_service_call("bettable-result/details", params={"gameID": gameID})
    

@app.get("/nosy/bettable-matches/opening-odds")
def nosy_bettable_matches_opening_odds(
    matchID: int = Query(..., description="Nosy MatchID (zorunlu)")
):
    # Açılış oranları (tek maç) - matchID şart
    return nosy_service_call("bettable-matches/opening-odds", params={"matchID": matchID})

# --- POOL SYNC ENDPOINTS ---
# Gerekenler: engine (SQLAlchemy), text (sqlalchemy.sql), datetime, timezone/ZoneInfo (TR saati), nosy_service_get(), _dump_json()
@app.post("/pool/bettable-matches/sync")
def sync_pool_bettable_matches():
    """
    NosyAPI -> bettable-matches
    Günün bültenini çekip pool_matches tablosuna upsert eder.
    """
    payload = nosy_service_call("bettable-matches")
    data = payload.get("data") or []
    received = len(data)

    fetched_at_tr = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

    upserted = 0
    skipped = 0

    with engine.begin() as conn:
        for item in data:
            if not isinstance(item, dict):
                skipped += 1
                continue

            mid = item.get("MatchID")
            try:
                mid = int(mid)
            except Exception:
                skipped += 1
                continue

            # Temel alanlar
            date_val = str(item.get("Date") or "")
            time_val = str(item.get("Time") or "")
            dt_val   = str(item.get("DateTime") or "")

            league  = str(item.get("League") or "")
            country = str(item.get("Country") or "")
            team1   = str(item.get("Team1") or "")
            team2   = str(item.get("Team2") or "")

            ms1 = item.get("HomeWin")
            ms0 = item.get("Draw")
            ms2 = item.get("AwayWin")
            alt25 = item.get("Under25")
            ust25 = item.get("Over25")
            betcount = item.get("BetCount")
            game_result = item.get("GameResult")

            conn.execute(
                text("""
                    INSERT INTO pool_matches(
                        nosy_match_id,
                        match_datetime, date, time,
                        league, country, team1, team2,
                        betcount, ms1, ms0, ms2, alt25, ust25,
                        fetched_at_tr, raw_json, game_result
                    )
                    VALUES(
                        :mid,
                        :dt, :date, :time,
                        :league, :country, :team1, :team2,
                        :betcount, :ms1, :ms0, :ms2, :alt25, :ust25,
                        :fetched_at_tr, :raw_json, :game_result
                    )
                    ON CONFLICT(nosy_match_id) DO UPDATE SET
                        match_datetime = EXCLUDED.match_datetime,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time,
                        league = EXCLUDED.league,
                        country = EXCLUDED.country,
                        team1 = EXCLUDED.team1,
                        team2 = EXCLUDED.team2,
                        betcount = EXCLUDED.betcount,
                        ms1 = EXCLUDED.ms1,
                        ms0 = EXCLUDED.ms0,
                        ms2 = EXCLUDED.ms2,
                        alt25 = EXCLUDED.alt25,
                        ust25 = EXCLUDED.ust25,
                        fetched_at_tr = EXCLUDED.fetched_at_tr,
                        raw_json = EXCLUDED.raw_json,
                        game_result = EXCLUDED.game_result
                """),
                {
                    "mid": mid,
                    "dt": dt_val,
                    "date": date_val,
                    "time": time_val,
                    "league": league,
                    "country": country,
                    "team1": team1,
                    "team2": team2,
                    "betcount": betcount,
                    "ms1": ms1,
                    "ms0": ms0,
                    "ms2": ms2,
                    "alt25": alt25,
                    "ust25": ust25,
                    "fetched_at_tr": fetched_at_tr,
                    "raw_json": _dump_json(item),
                    "game_result": game_result,
                }
            )
            upserted += 1

    return {
        "ok": True,
        "endpoint": "bettable-matches",
        "received": received,
        "upserted": upserted,
        "skipped": skipped,
        "fetched_at_tr": fetched_at_tr,
        "rowCount": payload.get("rowCount"),
        "creditUsed": payload.get("creditUsed"),
    }                 

@app.get("/pool/bettable-matches")
def get_pool_bettable_matches(
    day: Optional[str] = Query(None, description="YYYY-MM-DD. Boşsa en son bülten."),
    which: str = Query("latest", description="latest | oldest"),
    limit: int = Query(50, ge=1, le=500)
):
    """
    Pool'dan bülten listeler.
    - day yoksa: en son kaydedilen bülten (MAX fetched_at_tr)
    - day varsa: o günün en son bülteni (MAX fetched_at_tr WHERE fetched_at_tr LIKE 'YYYY-MM-DD%')
    - which=oldest: MIN fetched_at_tr (veya gün içindeki MIN)
    """
    with engine.begin() as conn:
        # 1) Hangi snapshot (fetched_at_tr) gösterilecek?
        if day:
            # Gün içindeki en son/en eski snapshot
            if which == "oldest":
                snap = conn.execute(text("""
                    SELECT MIN(fetched_at_tr) AS snap
                    FROM pool_matches
                    WHERE fetched_at_tr LIKE :daypat
                """), {"daypat": f"{day}%"}).mappings().first()
            else:
                snap = conn.execute(text("""
                    SELECT MAX(fetched_at_tr) AS snap
                    FROM pool_matches
                    WHERE fetched_at_tr LIKE :daypat
                """), {"daypat": f"{day}%"}).mappings().first()
        else:
            # Tüm zamanların en son/en eski snapshot
            if which == "oldest":
                snap = conn.execute(text("""
                    SELECT MIN(fetched_at_tr) AS snap
                    FROM pool_matches
                """)).mappings().first()
            else:
                snap = conn.execute(text("""
                    SELECT MAX(fetched_at_tr) AS snap
                    FROM pool_matches
                """)).mappings().first()

        snap_val = (snap or {}).get("snap")
        if not snap_val:
            return {"ok": True, "snapshot": None, "count": 0, "items": []}

        # 2) O snapshot'a ait maçları getir
        rows = conn.execute(text("""
            SELECT
                nosy_match_id,
                match_datetime, date, time,
                league, country, team1, team2,
                betcount, game_result, ms1, ms0, ms2, alt25, ust25,
                fetched_at_tr
            FROM pool_matches
            WHERE fetched_at_tr = :snap
            ORDER BY league, time, team1
            LIMIT :limit
        """), {"snap": snap_val, "limit": limit}).mappings().all()

    return {
        "ok": True,
        "day": day,
        "which": which,
        "snapshot": snap_val,
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }

@app.post("/db/finished-matches/sync")
def sync_finished_matches(
    backfill: int = Query(default=0, description="1 ise gece sarkan maçlar için details backfill yapar"),
    max_details: int = Query(default=25, ge=0, le=200, description="Backfill'de en fazla kaç maça details denenecek"),
):
    """
    NosyAPI -> matches-result (bulk)
    + opsiyonel: matches-result/details (backfill)

    KURAL:
    - SADECE GameResult belirleyici.
    - GameResult != 1 ise skor/kart/korner parse edilmez, DB'ye yazılmaz.
    - GameResult == 1 ise skor + diğer metalar parse edilir ve upsert edilir.
    - Result / LiveStatus güvenilmez; karar mekanizmasında kullanılmaz.
    """

    # -------------------------
    # SQL (tek yerde)
    # -------------------------
    UPSERT_SQL = text("""
        INSERT INTO finished_matches(
            nosy_match_id,
            match_datetime, date, time,
            league_code, league, country, team1, team2,
            betcount, ms1, ms0, ms2, alt25, ust25,
            ft_home, ft_away, ht_home, ht_away,
            home_corner, away_corner,
            home_yellow, away_yellow,
            home_red, away_red,
            mb, result, game_result, live_status,
            fetched_at_tr, raw_json,
            updated_at
        )
        VALUES(
            :mid,
            :dt, :date, :time,
            :league_code, :league, :country, :team1, :team2,
            :betcount, :ms1, :ms0, :ms2, :alt25, :ust25,
            :ft_home, :ft_away, :ht_home, :ht_away,
            :home_corner, :away_corner,
            :home_yellow, :away_yellow,
            :home_red, :away_red,
            :mb, :result, :game_result, :live_status,
            :fetched_at_tr, :raw_json,
            NOW()
        )
        ON CONFLICT(nosy_match_id) DO UPDATE SET
            match_datetime = EXCLUDED.match_datetime,
            date = EXCLUDED.date,
            time = EXCLUDED.time,
            league_code = EXCLUDED.league_code,
            league = EXCLUDED.league,
            country = EXCLUDED.country,
            team1 = EXCLUDED.team1,
            team2 = EXCLUDED.team2,
            betcount = EXCLUDED.betcount,
            ms1 = EXCLUDED.ms1,
            ms0 = EXCLUDED.ms0,
            ms2 = EXCLUDED.ms2,
            alt25 = EXCLUDED.alt25,
            ust25 = EXCLUDED.ust25,
            ft_home = EXCLUDED.ft_home,
            ft_away = EXCLUDED.ft_away,
            ht_home = EXCLUDED.ht_home,
            ht_away = EXCLUDED.ht_away,
            home_corner = EXCLUDED.home_corner,
            away_corner = EXCLUDED.away_corner,
            home_yellow = EXCLUDED.home_yellow,
            away_yellow = EXCLUDED.away_yellow,
            home_red = EXCLUDED.home_red,
            away_red = EXCLUDED.away_red,
            mb = EXCLUDED.mb,
            result = EXCLUDED.result,
            game_result = EXCLUDED.game_result,
            live_status = EXCLUDED.live_status,
            fetched_at_tr = EXCLUDED.fetched_at_tr,
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """)

    # -----------------------
    # 1) BULK: matches-result
    # -----------------------
    payload = nosy_service_call("matches-result")
    data = payload.get("data") or []
    received = len(data)

    fetched_at_tr = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

    bulk_report = {
        "received": received,
        "upserted": 0,
        "skipped": 0,
        "not_finished_gr0": 0,      # GameResult != 1 => işlem yapılmadı
        "gr1_but_no_score": 0,      # GameResult==1 ama FT skor yok
        "gr1_but_empty_matchResult": 0,
        "fetched_at_tr": fetched_at_tr,
        "rowCount": payload.get("rowCount"),
        "creditUsed": payload.get("creditUsed"),
    }
    bulk_debug = []  # ilk 10 örnek
    MAX_DEBUG = 10

    # -----------------------
    # 2) BACKFILL REPORT
    # -----------------------
    backfill_report = {
        "enabled": bool(backfill),
        "window": None,
        "candidates": 0,
        "requested": 0,
        "upserted": 0,
        "details_failed": 0,        # details data gelmedi
        "not_finished_gr0": 0,      # GameResult != 1 => işlem yapılmadı
        "gr1_but_no_score": 0,      # GameResult==1 ama FT skor yok
        "gr1_but_empty_matchResult": 0,
    }
    backfill_debug = []  # ilk 10 örnek

    with engine.begin() as conn:
        # -------- BULK LOOP --------
        for item in data:
            if not isinstance(item, dict):
                bulk_report["skipped"] += 1
                continue

            mid = item.get("MatchID")
            try:
                mid = int(mid)
            except Exception:
                bulk_report["skipped"] += 1
                continue

            gr_i = _gr_int(item)

            # >>>>> SADECE GameResult belirleyici <<<<<
            if gr_i != 1:
                bulk_report["not_finished_gr0"] += 1
                if len(bulk_debug) < MAX_DEBUG:
                    bulk_debug.append({
                        "stage": "bulk",
                        "mid": mid,
                        "why": "GameResult!=1 => parse yok",
                        "GameResult": item.get("GameResult"),
                        "matchResult_len": len(item.get("matchResult") or []),
                    })
                continue

            mr = item.get("matchResult") or []
            if not mr:
                bulk_report["gr1_but_empty_matchResult"] += 1
                if len(bulk_debug) < MAX_DEBUG:
                    bulk_debug.append({
                        "stage": "bulk",
                        "mid": mid,
                        "why": "GameResult==1 ama matchResult boş",
                        "GameResult": item.get("GameResult"),
                    })
                continue

            ft_home = _to_int_score(_meta_ci_get(mr, "msHomeScore"))
            ft_away = _to_int_score(_meta_ci_get(mr, "msAwayScore"))

            # GameResult==1 ama skor yoksa yazma
            if ft_home is None or ft_away is None:
                bulk_report["gr1_but_no_score"] += 1
                if len(bulk_debug) < MAX_DEBUG:
                    bulk_debug.append({
                        "stage": "bulk",
                        "mid": mid,
                        "why": "GameResult==1 ama FT skor yok",
                        "GameResult": item.get("GameResult"),
                        "sample_matchResult_first3": (mr[:3] if isinstance(mr, list) else []),
                    })
                continue

            # diğer metalar (opsiyonel)
            ht_home = _to_int_score(_meta_ci_get(mr, "htHomeScore"))
            ht_away = _to_int_score(_meta_ci_get(mr, "htAwayScore"))

            home_corner = _to_int_score(_meta_ci_get(mr, "homeCorner"))
            away_corner = _to_int_score(_meta_ci_get(mr, "awayCorner"))
            home_yellow = _to_int_score(_meta_ci_get(mr, "homeYellowCard"))
            away_yellow = _to_int_score(_meta_ci_get(mr, "awayYellowCard"))
            home_red = _to_int_score(_meta_ci_get(mr, "homeRedCard"))
            away_red = _to_int_score(_meta_ci_get(mr, "awayRedCard"))

            conn.execute(
                UPSERT_SQL,
                {
                    "mid": mid,
                    "dt": str(item.get("DateTime") or ""),
                    "date": str(item.get("Date") or ""),
                    "time": str(item.get("Time") or ""),
                    "league_code": str(item.get("LeagueCode") or ""),
                    "league": str(item.get("League") or ""),
                    "country": str(item.get("Country") or ""),
                    "team1": str(item.get("Team1") or ""),
                    "team2": str(item.get("Team2") or ""),
                    "betcount": item.get("BetCount"),
                    "ms1": item.get("HomeWin"),
                    "ms0": item.get("Draw"),
                    "ms2": item.get("AwayWin"),
                    "alt25": item.get("Under25"),
                    "ust25": item.get("Over25"),
                    "ft_home": ft_home,
                    "ft_away": ft_away,
                    "ht_home": ht_home,
                    "ht_away": ht_away,
                    "home_corner": home_corner,
                    "away_corner": away_corner,
                    "home_yellow": home_yellow,
                    "away_yellow": away_yellow,
                    "home_red": home_red,
                    "away_red": away_red,
                    "mb": item.get("MB"),
                    "result": item.get("Result"),       # zorunlu değil
                    "game_result": gr_i,               # zorunlu (1)
                    "live_status": item.get("LiveStatus"),  # karar için kullanılmaz
                    "fetched_at_tr": fetched_at_tr,
                    "raw_json": _dump_json(item),
                }
            )
            bulk_report["upserted"] += 1

        # -------- BACKFILL (optional) --------
        if int(backfill) == 1 and int(max_details) > 0:
            now_tr = datetime.now(TR_TZ) if TR_TZ else datetime.utcnow()
            yesterday = (now_tr.date() - timedelta(days=1)).isoformat()
            t_from = "22:00:00"
            t_to = "23:59:59"
            backfill_report["window"] = {"day": yesterday, "time_from": t_from, "time_to": t_to}

            mids = (
                conn.execute(
                    text("""
                        SELECT p.nosy_match_id
                        FROM pool_matches p
                        LEFT JOIN finished_matches f
                          ON f.nosy_match_id = p.nosy_match_id
                        WHERE
                          p.date = :day
                          AND p.time >= :t_from
                          AND p.time <= :t_to
                          AND (
                            f.nosy_match_id IS NULL
                            OR f.ft_home IS NULL
                            OR f.ft_away IS NULL
                          )
                        ORDER BY p.time ASC, p.nosy_match_id
                        LIMIT :lim
                    """),
                    {"day": yesterday, "t_from": t_from, "t_to": t_to, "lim": int(max_details)},
                )
                .scalars()
                .all()
            )

            backfill_report["candidates"] = len(mids)

            for mid in mids:
                backfill_report["requested"] += 1

                details_payload = nosy_service_call(
                    "matches-result/details",
                    params={"matchID": int(mid), "match_id": int(mid)},
                )
                item = _take_details_data(details_payload)

                if not isinstance(item, dict):
                    backfill_report["details_failed"] += 1
                    if len(backfill_debug) < MAX_DEBUG:
                        backfill_debug.append({"stage": "backfill", "mid": int(mid), "why": "details_failed"})
                    continue

                gr_i = _gr_int(item)

                # >>>>> SADECE GameResult belirleyici <<<<<
                if gr_i != 1:
                    backfill_report["not_finished_gr0"] += 1
                    if len(backfill_debug) < MAX_DEBUG:
                        backfill_debug.append({
                            "stage": "backfill",
                            "mid": int(mid),
                            "why": "GameResult!=1 => parse yok",
                            "GameResult": item.get("GameResult"),
                            "matchResult_len": len(item.get("matchResult") or []),
                        })
                    continue

                mr = item.get("matchResult") or []
                if not mr:
                    backfill_report["gr1_but_empty_matchResult"] += 1
                    if len(backfill_debug) < MAX_DEBUG:
                        backfill_debug.append({
                            "stage": "backfill",
                            "mid": int(mid),
                            "why": "GameResult==1 ama matchResult boş",
                            "GameResult": item.get("GameResult"),
                        })
                    continue

                ft_home = _to_int_score(_meta_ci_get(mr, "msHomeScore"))
                ft_away = _to_int_score(_meta_ci_get(mr, "msAwayScore"))

                if ft_home is None or ft_away is None:
                    backfill_report["gr1_but_no_score"] += 1
                    if len(backfill_debug) < MAX_DEBUG:
                        backfill_debug.append({
                            "stage": "backfill",
                            "mid": int(mid),
                            "why": "GameResult==1 ama FT skor yok",
                            "GameResult": item.get("GameResult"),
                            "sample_matchResult_first3": (mr[:3] if isinstance(mr, list) else []),
                        })
                    continue

                # diğer metalar (opsiyonel)
                ht_home = _to_int_score(_meta_ci_get(mr, "htHomeScore"))
                ht_away = _to_int_score(_meta_ci_get(mr, "htAwayScore"))

                home_corner = _to_int_score(_meta_ci_get(mr, "homeCorner"))
                away_corner = _to_int_score(_meta_ci_get(mr, "awayCorner"))
                home_yellow = _to_int_score(_meta_ci_get(mr, "homeYellowCard"))
                away_yellow = _to_int_score(_meta_ci_get(mr, "awayYellowCard"))
                home_red = _to_int_score(_meta_ci_get(mr, "homeRedCard"))
                away_red = _to_int_score(_meta_ci_get(mr, "awayRedCard"))

                fetched_at_tr_bf = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

                conn.execute(
                    UPSERT_SQL,
                    {
                        "mid": int(mid),
                        "dt": str(item.get("DateTime") or ""),
                        "date": str(item.get("Date") or ""),
                        "time": str(item.get("Time") or ""),
                        "league_code": str(item.get("LeagueCode") or ""),
                        "league": str(item.get("League") or ""),
                        "country": str(item.get("Country") or ""),
                        "team1": str(item.get("Team1") or ""),
                        "team2": str(item.get("Team2") or ""),
                        "betcount": item.get("BetCount"),
                        "ms1": item.get("HomeWin"),
                        "ms0": item.get("Draw"),
                        "ms2": item.get("AwayWin"),
                        "alt25": item.get("Under25"),
                        "ust25": item.get("Over25"),
                        "ft_home": ft_home,
                        "ft_away": ft_away,
                        "ht_home": ht_home,
                        "ht_away": ht_away,
                        "home_corner": home_corner,
                        "away_corner": away_corner,
                        "home_yellow": home_yellow,
                        "away_yellow": away_yellow,
                        "home_red": home_red,
                        "away_red": away_red,
                        "mb": item.get("MB"),
                        "result": item.get("Result"),       # zorunlu değil
                        "game_result": gr_i,               # zorunlu (1)
                        "live_status": item.get("LiveStatus"),  # karar için kullanılmaz
                        "fetched_at_tr": fetched_at_tr_bf,
                        "raw_json": _dump_json(item),
                    }
                )
                backfill_report["upserted"] += 1

    return {
        "ok": True,
        "endpoint": "matches-result",
        "bulk": bulk_report,
        "bulk_debug": bulk_debug,
        "backfill": backfill_report,
        "backfill_debug": backfill_debug,
    }

@app.get("/db/finished-matches")
def list_finished_matches(
    day: Optional[str] = Query(default=None, description="YYYY-MM-DD. Boşsa en son snapshot."),
    which: str = Query(default="latest", description="latest | oldest"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Finished matches listesi.
    - day boşsa: en son (veya oldest seçilirse ilk) snapshot'tan limit kadar döner
    - day doluysa: o günün biten maçlarını döner
    """
    which = (which or "latest").lower().strip()
    if which not in ("latest", "oldest"):
        which = "latest"

    snap_sql = "MAX" if which == "latest" else "MIN"

    def _decorate_rows(rows):
        items = []
        for r in rows:
            d = dict(r)

            # skor stringleri
            d["ft"] = f'{d["ft_home"]}-{d["ft_away"]}' if d.get("ft_home") is not None and d.get("ft_away") is not None else None
            d["ht"] = f'{d["ht_home"]}-{d["ht_away"]}' if d.get("ht_home") is not None and d.get("ht_away") is not None else None

            # corners
            hc, ac = d.get("home_corner"), d.get("away_corner")
            if hc is not None and ac is not None:
                d["corners_total"] = hc + ac
                d["corners"] = f"{hc}-{ac}"
            else:
                d["corners_total"] = None
                d["corners"] = None

            # yellow cards
            hy, ay = d.get("home_yellow"), d.get("away_yellow")
            if hy is not None and ay is not None:
                d["yc_total"] = hy + ay
                d["yc"] = f"{hy}-{ay}"
            else:
                d["yc_total"] = None
                d["yc"] = None

            # red cards
            hr, ar = d.get("home_red"), d.get("away_red")
            if hr is not None and ar is not None:
                d["rc_total"] = hr + ar
                d["rc"] = f"{hr}-{ar}"
            else:
                d["rc_total"] = None
                d["rc"] = None

            items.append(d)
        return items

    with engine.begin() as conn:
        if day:
            rows = conn.execute(
                text("""
                    SELECT
                        nosy_match_id,
                        league, team1, team2,
                        date, time,
                        ms1, ms0, ms2, alt25, ust25,
                        ft_home, ft_away, ht_home, ht_away,
                        home_corner, away_corner,
                        home_yellow, away_yellow,
                        home_red, away_red,
                        betcount,
                        fetched_at_tr,
                        updated_at
                    FROM finished_matches
                    WHERE date = :day
                    ORDER BY match_datetime NULLS LAST, nosy_match_id
                    LIMIT :limit
                """),
                {"day": day, "limit": limit},
            ).mappings().all()

            items = _decorate_rows(rows)
            return {
                "ok": True,
                "day": day,
                "which": None,
                "count": len(items),
                "items": items,
            }

        snapshot = conn.execute(
            text(f"SELECT {snap_sql}(fetched_at_tr) AS snap FROM finished_matches")
        ).scalar()

        if not snapshot:
            return {"ok": True, "day": None, "which": which, "snapshot": None, "count": 0, "items": []}

        rows = conn.execute(
            text("""
                SELECT
                    nosy_match_id,
                    league, team1, team2,
                    date, time,
                    ms1, ms0, ms2, alt25, ust25,
                    ft_home, ft_away, ht_home, ht_away,
                    home_corner, away_corner,
                    home_yellow, away_yellow,
                    home_red, away_red,
                    betcount,
                    fetched_at_tr,
                    updated_at
                FROM finished_matches
                WHERE fetched_at_tr = :snapshot
                ORDER BY match_datetime NULLS LAST, nosy_match_id
                LIMIT :limit
            """),
            {"snapshot": snapshot, "limit": limit},
        ).mappings().all()

        items = _decorate_rows(rows)
        return {
            "ok": True,
            "day": None,
            "which": which,
            "snapshot": snapshot,
            "count": len(items),
            "items": items,
            }

@app.get("/health/metrics")
def health_metrics():
    with engine.begin() as conn:
        # -----------------
        # POOL
        # -----------------
        pool_total = conn.execute(
            text("SELECT COUNT(*) AS c FROM pool_matches")
        ).mappings().first()["c"]

        pool_latest = conn.execute(
            text("SELECT MAX(fetched_at_tr) AS mx FROM pool_matches")
        ).mappings().first()["mx"]

        pool_latest_count = 0
        if pool_latest:
            pool_latest_count = conn.execute(
                text("SELECT COUNT(*) AS c FROM pool_matches WHERE fetched_at_tr = :mx"),
                {"mx": pool_latest}
            ).mappings().first()["c"]

        # -----------------
        # FINISHED
        # -----------------
        finished_total = conn.execute(
            text("SELECT COUNT(*) AS c FROM finished_matches")
        ).mappings().first()["c"]

        finished_latest = conn.execute(
            text("SELECT MAX(fetched_at_tr) AS mx FROM finished_matches")
        ).mappings().first()["mx"]

        finished_latest_count = 0
        if finished_latest:
            finished_latest_count = conn.execute(
                text("SELECT COUNT(*) AS c FROM finished_matches WHERE fetched_at_tr = :mx"),
                {"mx": finished_latest}
            ).mappings().first()["c"]

    return {
        "ok": True,
        "pool": {
            "total_in_db": int(pool_total),
            "latest_snapshot": pool_latest,
            "latest_snapshot_count": int(pool_latest_count),
        },
        "finished": {
            "total_in_db": int(finished_total),
            "latest_snapshot": finished_latest,
            "latest_snapshot_count": int(finished_latest_count),
        }
    }

# ------------------------------------------------------------
# League Profile Stats
# finished_matches -> aggregation -> panel context
# ------------------------------------------------------------

@app.get("/stats/league-profile")
def stats_league_profile(
    league_code: str = Query(..., min_length=1),
    league: str = Query(..., min_length=1),
    min_matches: int = Query(10, ge=1, le=5000),
):
    """
    finished_matches tablosundan lig bazlı profil (V1) üretir.
    - league_code + league zorunlu
    - default all-time
    """

    def _rate(cnt: int, total: int) -> float:
        if not total:
            return 0.0
        return float(cnt) / float(total)

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT
                    COUNT(*)::int AS match_count,

                    -- 1X2 (FT skoruna göre)
                    COUNT(*) FILTER (WHERE ft_home > ft_away)::int AS home_win_count,
                    COUNT(*) FILTER (WHERE ft_home = ft_away)::int AS draw_count,
                    COUNT(*) FILTER (WHERE ft_home < ft_away)::int AS away_win_count,

                    -- goals
                    AVG((ft_home + ft_away)::numeric) AS avg_goals_total,
                    COUNT(*) FILTER (WHERE (ft_home + ft_away) >= 3)::int AS over25_count,
                    COUNT(*) FILTER (WHERE ft_home > 0 AND ft_away > 0)::int AS btts_count,

                    -- goal distribution buckets
                    COUNT(*) FILTER (WHERE (ft_home + ft_away) BETWEEN 0 AND 1)::int AS g0_1_count,
                    COUNT(*) FILTER (WHERE (ft_home + ft_away) BETWEEN 2 AND 3)::int AS g2_3_count,
                    COUNT(*) FILTER (WHERE (ft_home + ft_away) BETWEEN 4 AND 5)::int AS g4_5_count,
                    COUNT(*) FILTER (WHERE (ft_home + ft_away) >= 6)::int AS g6p_count,

                    -- corners
                    AVG((COALESCE(home_corner,0) + COALESCE(away_corner,0))::numeric) AS avg_corners_total,
                    AVG(COALESCE(home_corner,0)::numeric) AS avg_home_corner,
                    AVG(COALESCE(away_corner,0)::numeric) AS avg_away_corner,

                    -- yellow cards
                    AVG((COALESCE(home_yellow,0) + COALESCE(away_yellow,0))::numeric) AS avg_yellow_total,
                    AVG(COALESCE(home_yellow,0)::numeric) AS avg_yellow_home,
                    AVG(COALESCE(away_yellow,0)::numeric) AS avg_yellow_away,

                    -- red cards (match-level)
                    COUNT(*) FILTER (
                        WHERE (COALESCE(home_red,0) + COALESCE(away_red,0)) > 0
                    )::int AS red_match_count

                FROM finished_matches
                WHERE league_code = :league_code
                  AND league = :league
            """),
            {"league_code": league_code, "league": league},
        ).mappings().first()

    # Eğer hiç maç yoksa boş profil döndür
    if not row:
        return {
            "ok": True,
            "league_code": league_code,
            "league": league,
            "sample": {"match_count": 0, "low_sample": True, "min_matches": min_matches},
            "one_x_two": {
                "home": {"count": 0, "rate": 0.0},
                "draw": {"count": 0, "rate": 0.0},
                "away": {"count": 0, "rate": 0.0},
            },
            "goals": {
                "avg_total": 0.0,
                "over25": {"count": 0, "rate": 0.0},
                "btts": {"count": 0, "rate": 0.0},
                "distribution": {
                    "g0_1": {"count": 0, "rate": 0.0},
                    "g2_3": {"count": 0, "rate": 0.0},
                    "g4_5": {"count": 0, "rate": 0.0},
                    "g6p": {"count": 0, "rate": 0.0},
                },
            },
            "corners": {"avg_total": 0.0, "avg_home": 0.0, "avg_away": 0.0},
            "cards": {
                "avg_yellow_total": 0.0,
                "avg_yellow_home": 0.0,
                "avg_yellow_away": 0.0,
                "red_match": {"count": 0, "rate": 0.0},
            },
        }

    match_count = int(row["match_count"] or 0)

    # counts
    home_win_count = int(row["home_win_count"] or 0)
    draw_count = int(row["draw_count"] or 0)
    away_win_count = int(row["away_win_count"] or 0)

    over25_count = int(row["over25_count"] or 0)
    btts_count = int(row["btts_count"] or 0)

    g0_1_count = int(row["g0_1_count"] or 0)
    g2_3_count = int(row["g2_3_count"] or 0)
    g4_5_count = int(row["g4_5_count"] or 0)
    g6p_count = int(row["g6p_count"] or 0)

    red_match_count = int(row["red_match_count"] or 0)

    # avgs (numeric -> float)
    avg_goals_total = float(row["avg_goals_total"] or 0.0)

    avg_corners_total = float(row["avg_corners_total"] or 0.0)
    avg_home_corner = float(row["avg_home_corner"] or 0.0)
    avg_away_corner = float(row["avg_away_corner"] or 0.0)

    avg_yellow_total = float(row["avg_yellow_total"] or 0.0)
    avg_yellow_home = float(row["avg_yellow_home"] or 0.0)
    avg_yellow_away = float(row["avg_yellow_away"] or 0.0)

    low_sample = match_count < int(min_matches)

    return {
        "ok": True,
        "league_code": league_code,
        "league": league,
        "sample": {
            "match_count": match_count,
            "low_sample": low_sample,
            "min_matches": int(min_matches),
        },
        "one_x_two": {
            "home": {"count": home_win_count, "rate": _rate(home_win_count, match_count)},
            "draw": {"count": draw_count, "rate": _rate(draw_count, match_count)},
            "away": {"count": away_win_count, "rate": _rate(away_win_count, match_count)},
        },
        "goals": {
            "avg_total": avg_goals_total,
            "over25": {"count": over25_count, "rate": _rate(over25_count, match_count)},
            "btts": {"count": btts_count, "rate": _rate(btts_count, match_count)},
            "distribution": {
                "g0_1": {"count": g0_1_count, "rate": _rate(g0_1_count, match_count)},
                "g2_3": {"count": g2_3_count, "rate": _rate(g2_3_count, match_count)},
                "g4_5": {"count": g4_5_count, "rate": _rate(g4_5_count, match_count)},
                "g6p": {"count": g6p_count, "rate": _rate(g6p_count, match_count)},
            },
        },
        "corners": {
            "avg_total": avg_corners_total,
            "avg_home": avg_home_corner,
            "avg_away": avg_away_corner,
        },
        "cards": {
            "avg_yellow_total": avg_yellow_total,
            "avg_yellow_home": avg_yellow_home,
            "avg_yellow_away": avg_yellow_away,
            "red_match": {"count": red_match_count, "rate": _rate(red_match_count, match_count)},
        },
              }

@app.get("/stats/organizations")
def stats_organizations(
    min_matches: int = Query(1, ge=1, le=100000),
    limit: int = Query(200, ge=1, le=1000),
):
    """
    finished_matches içinden organizasyon (league_code + league) listesini döner.
    Her organizasyon için toplam maç sayısı ve ilk/son tarih.
    """

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    league_code,
                    league,
                    COUNT(*)::int AS match_count,
                    MIN(date) AS first_date,
                    MAX(date) AS last_date
                FROM finished_matches
                GROUP BY league_code, league
                HAVING COUNT(*) >= :min_matches
                ORDER BY match_count DESC
                LIMIT :limit
            """),
            {"min_matches": min_matches, "limit": limit},
        ).mappings().all()

    return {
        "ok": True,
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }


# ==========================================================
# Flashscore API Layer (RapidAPI) - separated in Swagger
# ==========================================================

# ---------------------------
# Config (ENV) - FLASHSCORE (RapidAPI)
# ---------------------------
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()

FLASHSCORE_BASE_URL = os.getenv(
    "FLASHSCORE_BASE_URL",
    "https://flashscore4.p.rapidapi.com/api/flashscore/v1"
).strip().rstrip("/")

FLASHSCORE_RAPIDAPI_HOST = os.getenv(
    "FLASHSCORE_RAPIDAPI_HOST",
    "flashscore4.p.rapidapi.com"
).strip()

# ---------------------------
# FLASHSCORE HELPER
# ---------------------------
def _require_rapidapi_key():
    if not RAPIDAPI_KEY:
        raise HTTPException(status_code=500, detail="RAPIDAPI_KEY env eksik.")

def flashscore_call(endpoint: str, *, params: dict | None = None) -> dict:
    """
    RapidAPI Flashscore çağrısı.
    Base: https://flashscore4.p.rapidapi.com/api/flashscore/v1
    Header zorunlu: x-rapidapi-key, x-rapidapi-host
    """
    _require_rapidapi_key()

    url = _join_url(FLASHSCORE_BASE_URL, endpoint)

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

def flashscore_base_check() -> dict:
    """
    Flashscore RapidAPI base URL'ye GET atar (sonunda /ping yok).
    Sadece rate limit header'larını ve TR saat bilgilerini döndürür.
    """
    _require_rapidapi_key()

    url = FLASHSCORE_BASE_URL  # <- SONUNDA /ping YOK

    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": FLASHSCORE_RAPIDAPI_HOST,
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Flashscore bağlantı hatası: {e}")

    # Upstream hata ise body'i de gösterelim (debug için)
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise HTTPException(
            status_code=r.status_code,
            detail={"url": url, "body": body}
        )

    # Rate-limit header'larını topla
    h = {k.lower(): v for k, v in r.headers.items()}
    limit = h.get("x-ratelimit-requests-limit")
    remaining = h.get("x-ratelimit-requests-remaining")
    reset_raw = h.get("x-ratelimit-requests-reset")  # bazen epoch saniye gibi gelir

    now_tr = datetime.now(TR_TZ)

    reset_tr_iso = None
    reset_in_seconds = None

    # reset epoch ise TR saatine çevirelim
    if reset_raw and reset_raw.isdigit():
        reset_epoch = int(reset_raw)
        reset_dt_tr = datetime.fromtimestamp(reset_epoch, tz=timezone.utc).astimezone(TR_TZ)
        reset_tr_iso = reset_dt_tr.isoformat()

        # kaç saniye kaldı (negatifse 0 yap)
        reset_in_seconds = max(0, int((reset_dt_tr - now_tr).total_seconds()))

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
                "seconds_until_reset": reset_in_seconds,
            },
            "turkey_time": now_tr.isoformat(),
        },
    }

def flashscore_get(path: str):
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
        r = requests.get(url, headers=headers, timeout=30)
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

@app.get("/flashscore/country", tags=["Flashscore"])
def flashscore_countries():
    # Senin verdiğin gerçek endpoint:
    # /general/1/countries
    return flashscore_get("general/1/countries")

@app.get("/flashscore/tournaments/{country_id}", tags=["Flashscore"])
def flashscore_country_tournaments(
    country_id: int,
    sport_id: int = Query(1, description="Sport ID (football=1)")
):
    # Upstream path: /general/{sport_id}/{country_id}/tournaments
    path = f"general/{sport_id}/{country_id}/tournaments"
    return flashscore_get(path)

@app.get("/flashscore/matches/{date}", tags=["Flashscore"])
def flashscore_matches_by_date(date: str):
    """
    Tarihe göre maç listesi çeker.
    Upstream: /match/list/1/{date}
    """
    sport_id = 1  # futbol

    # API URL’de futbol için ilk 1 sabit:
    path = f"match/list/{sport_id}/{date}"

    return flashscore_get(path)

@app.get("/flashscore/matches/day/{offset}", tags=["Flashscore"])
def flashscore_matches_for_offset(offset: int):
    """
    -7 to 7 arası gün offsetine göre maç listesi.
    Upstream: /match/list/1/{offset}
    """
    if offset < -7 or offset > 7:
        raise HTTPException(status_code=400, detail="Offset -7 ile 7 arasında olmalı.")

    sport_id = 1

    return flashscore_get(f"match/list/{sport_id}/{offset}")

@app.get("/flashscore/match/{match_id}", tags=["Flashscore"])
def flashscore_match(match_id: str):
    return flashscore_get(f"match/details/{match_id}")

@app.get("/flashscore/match-odds/{match_id}", tags=["Flashscore"])
def flashscore_match_odds(match_id: str):
    """
    Tek maça ait oranları çeker.
    Upstream: /match/odds/{match_id}
    Örn match_id: GCxZ2uHc
    """
    return flashscore_get(f"match/odds/{match_id}")

@app.get("/flashscore/match-stats/{match_id}", tags=["Flashscore"])
def flashscore_match_stats(match_id: str):
    """
    Tek maça ait istatistikleri çeker.
    Upstream: /match/stats/{match_id}
    Örn match_id: GCxZ2uHc
    """
    return flashscore_get(f"match/stats/{match_id}")

@app.get("/flashscore/match-commentary/{match_id}", tags=["Flashscore"])
def flashscore_match_commentary(match_id: str):
    """
    Tek maça ait anlatım verilerini çeker.
    Upstream: /match/comumentary/{match_id}
    Örn match_id: GCxZ2uHc
    """
    return flashscore_get(f"match/comumentary/{match_id}")

@app.get("/flashscore/match-standings/{match_id}/{type}", tags=["Flashscore"])
def flashscore_match_standings(match_id: str, type: str):
    """
    Maça göre puan durumu tablosunu çeker.
    type enum: overall, home, away
    """
    type = type.lower()

    if type not in ["overall", "home", "away"]:
        raise HTTPException(status_code=400, detail="Type overall, home veya away olmalı.")

    # Upstream path:
    # match/standings-table/{match_id}/{type}
    return flashscore_get(f"match/standings-table/{match_id}/{type}")

@app.get("/flashscore/match-standings-form/{match_id}/{type}", tags=["Flashscore"])
def flashscore_standings_form(match_id: str, type: str):
    """
    Maçın form (son maçlar performansı) istatistiklerini çeker.
    type enum: overall, home, away
    """
    type = type.lower()

    if type not in ["overall", "home", "away"]:
        raise HTTPException(status_code=400, detail="Type overall, home veya away olmalı.")

    return flashscore_get(f"match/standings-form/{match_id}/{type}")

# ==========================================================
# Flashscore -> DB (Finished + MS odds only) - Phase 1
# ==========================================================

def _safe_int(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s == "-" or s.lower() == "null":
            return None
        return int(s)
    except Exception:
        return None

def _safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", ".")
        if s == "" or s == "-" or s.lower() == "null":
            return None
        return float(s)
    except Exception:
        return None

def _fs_ts_to_tr(ts: int) -> Tuple[str, str, str]:
    """UNIX timestamp -> (match_datetime_tr_iso, date_str, time_str) in TR"""
    if TR_TZ:
        dtr = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(TR_TZ)
    else:
        dtr = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    iso = dtr.isoformat()
    return iso, dtr.date().isoformat(), dtr.time().replace(microsecond=0).isoformat()

def _fs_pick_blocks(payload: Any) -> List[Dict[str, Any]]:
    """Flashscore match/list payload shape can vary. Normalize to list of blocks."""
    if isinstance(payload, list):
        return [b for b in payload if isinstance(b, dict)]
    if isinstance(payload, dict):
        for k in ("data", "response", "items", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return [b for b in v if isinstance(b, dict)]
        # sometimes payload itself is a block
        return [payload]
    return []

def _fs_iter_matches(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    ms = block.get("matches") or block.get("data") or []
    if isinstance(ms, list):
        return [m for m in ms if isinstance(m, dict)]
    return []

def _fs_is_finished(m: Dict[str, Any]) -> bool:
    return (str(m.get("stage") or "").strip().lower() == "finished")

def _fs_pick_ms_odds(m: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    odds = m.get("odds") or {}
    if not isinstance(odds, dict):
        return None, None, None
    ms1 = _safe_float(odds.get("1"))
    ms0 = _safe_float(odds.get("X"))
    ms2 = _safe_float(odds.get("2"))
    return ms1, ms0, ms2

@app.post("/flashscore/db/finished-ms/sync", tags=["Flashscore"])
def flashscore_finished_ms_sync(
    day: int = Query(0, ge=-7, le=7, description="0=today, -1=yesterday, +1=tomorrow (Flashscore day offset)"),
):
    """
    Phase-1:
    Flashscore match/list -> SADECE stage=Finished + MS(1/X/2) var + FT skor var
    DB: flash_finished_ms tablosuna upsert eder.
    """
    ensure_schema()

    fetched_at_tr = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

    payload = flashscore_get(f"match/list/1/{day}")
    blocks = _fs_pick_blocks(payload)

    received = 0
    finished = 0
    finished_with_ms = 0
    upserted = 0
    skipped = 0

    UPSERT = text("""
        INSERT INTO flash_finished_ms(
            flash_match_id,
            match_datetime_tr, date, time,
            country_name, tournament_name,
            home, away,
            ft_home, ft_away,
            ms1, ms0, ms2,
            fetched_at_tr, raw_json,
            updated_at
        )
        VALUES(
            :flash_match_id,
            :match_datetime_tr, :date, :time,
            :country_name, :tournament_name,
            :home, :away,
            :ft_home, :ft_away,
            :ms1, :ms0, :ms2,
            :fetched_at_tr, :raw_json,
            NOW()
        )
        ON CONFLICT(flash_match_id) DO UPDATE SET
            match_datetime_tr = EXCLUDED.match_datetime_tr,
            date = EXCLUDED.date,
            time = EXCLUDED.time,
            country_name = EXCLUDED.country_name,
            tournament_name = EXCLUDED.tournament_name,
            home = EXCLUDED.home,
            away = EXCLUDED.away,
            ft_home = EXCLUDED.ft_home,
            ft_away = EXCLUDED.ft_away,
            ms1 = EXCLUDED.ms1,
            ms0 = EXCLUDED.ms0,
            ms2 = EXCLUDED.ms2,
            fetched_at_tr = EXCLUDED.fetched_at_tr,
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """)

    with engine.begin() as conn:
        for b in blocks:
            for m in _fs_iter_matches(b):
                received += 1

                if not _fs_is_finished(m):
                    continue
                finished += 1

                ms1, ms0, ms2 = _fs_pick_ms_odds(m)
                if ms1 is None or ms0 is None or ms2 is None:
                    continue

                ht = m.get("home_team") or {}
                at = m.get("away_team") or {}
                if not isinstance(ht, dict) or not isinstance(at, dict):
                    continue

                ft_home = _safe_int(ht.get("score"))
                ft_away = _safe_int(at.get("score"))
                if ft_home is None or ft_away is None:
                    continue

                match_id = m.get("match_id")
                ts = m.get("timestamp")
                if not match_id or ts is None:
                    skipped += 1
                    continue

                finished_with_ms += 1

                match_dt_tr, date_str, time_str = _fs_ts_to_tr(ts)

                country_name = b.get("country_name") or b.get("country") or None
                tournament_name = b.get("name") or b.get("tournament_name") or None

                conn.execute(
                    UPSERT,
                    {
                        "flash_match_id": str(match_id),
                        "match_datetime_tr": match_dt_tr,
                        "date": date_str,
                        "time": time_str,
                        "country_name": (str(country_name) if country_name is not None else None),
                        "tournament_name": (str(tournament_name) if tournament_name is not None else None),
                        "home": str(ht.get("name") or ""),
                        "away": str(at.get("name") or ""),
                        "ft_home": ft_home,
                        "ft_away": ft_away,
                        "ms1": ms1,
                        "ms0": ms0,
                        "ms2": ms2,
                        "fetched_at_tr": fetched_at_tr,
                        "raw_json": _dump_json(m),
                    }
                )
                upserted += 1

    return {
        "ok": True,
        "day": day,
        "received": received,
        "finished": finished,
        "finished_with_ms": finished_with_ms,
        "upserted": upserted,
        "skipped": skipped,
        "fetched_at_tr": fetched_at_tr,
    }

@app.get("/flashscore/db/finished-ms", tags=["Flashscore"])
def flashscore_finished_ms_list(
    day: Optional[str] = Query(default=None, description="YYYY-MM-DD (opsiyonel)"),
    which: str = Query(default="latest", description="latest | oldest"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    flash_finished_ms listesini DB'den döner.
    - day verilirse: o günün kayıtları
    - day yoksa: snapshot (fetched_at_tr) bazlı latest/oldest
    """
    ensure_schema()

    which = (which or "latest").lower().strip()
    if which not in ("latest", "oldest"):
        which = "latest"

    snap_sql = "MAX" if which == "latest" else "MIN"

    with engine.begin() as conn:
        if day:
            rows = conn.execute(
                text("""
                    SELECT
                        flash_match_id,
                        match_datetime_tr, date, time,
                        country_name, tournament_name,
                        home, away,
                        ft_home, ft_away,
                        ms1, ms0, ms2,
                        fetched_at_tr,
                        updated_at
                    FROM flash_finished_ms
                    WHERE date = :day
                    ORDER BY match_datetime_tr NULLS LAST, flash_match_id
                    LIMIT :limit
                """),
                {"day": day, "limit": limit},
            ).mappings().all()

            return {"ok": True, "day": day, "which": None, "count": len(rows), "items": [dict(r) for r in rows]}

        snapshot = conn.execute(text(f"SELECT {snap_sql}(fetched_at_tr) AS snap FROM flash_finished_ms")).scalar()

        if not snapshot:
            return {"ok": True, "day": None, "which": which, "snapshot": None, "count": 0, "items": []}

        rows = conn.execute(
            text("""
                SELECT
                    flash_match_id,
                    match_datetime_tr, date, time,
                    country_name, tournament_name,
                    home, away,
                    ft_home, ft_away,
                    ms1, ms0, ms2,
                    fetched_at_tr,
                    updated_at
                FROM flash_finished_ms
                WHERE fetched_at_tr = :snapshot
                ORDER BY match_datetime_tr NULLS LAST, flash_match_id
                LIMIT :limit
            """),
            {"snapshot": snapshot, "limit": limit},
        ).mappings().all()

    return {
        "ok": True,
        "day": None,
        "which": which,
        "snapshot": snapshot,
        "count": len(rows),
        "items": [dict(r) for r in rows],
        }

