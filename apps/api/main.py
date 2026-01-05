import os
import requests
import json
import datetime as dt

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime
from zoneinfo import ZoneInfo

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
    """
    Dict / list gibi yapıları güvenli şekilde JSON stringe çevirir.
    Pool katmanı için yeterli.
    """
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def _require_api_key():
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env eksik.")

def _join_url(base: str, endpoint: str) -> str:
    base = (base or "").rstrip("/")
    endpoint = (endpoint or "").lstrip("/")
    return f"{base}/{endpoint}"

def nosy_service_call(endpoint: str, *, params: dict | None = None) -> dict:
    """
    SERVICE base üzerinden çağrı:
    https://www.nosyapi.com/apiv2/service/<endpoint>
    """
    _require_api_key()
    url = _join_url(NOSY_SERVICE_BASE_URL, endpoint)

    q = dict(params or {})
    q["apiKey"] = NOSY_API_KEY

    try:
        r = requests.get(url, params=q, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Nosy bağlantı hatası: {e}")

    # Nosy bazen 200 dönüp status=failure verir; o yüzden json’u döndürüp üstte kontrol etmek daha iyi.
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
    """
    CHECK base üzerinden çağrı:
    https://www.nosyapi.com/apiv2/nosy-service/check?apiKey=...&apiID=...
    """
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
        conn.execute(text("""
            ALTER TABLE pool_matches
            ADD COLUMN IF NOT EXISTS fetched_at_tr TEXT;
        """))

        conn.execute(text("""
            ALTER TABLE pool_matches
            ADD COLUMN IF NOT EXISTS raw_json TEXT;
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS matches (
                id BIGSERIAL PRIMARY KEY,
                nosy_match_id BIGINT NOT NULL UNIQUE,
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
                source_fetched_at_tr TEXT,
                pool_raw_json TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """))
        
        # telefon/console yoksa güvenli schema patch
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS source_fetched_at_tr TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS pool_raw_json TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS ust25 DOUBLE PRECISION;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS alt25 DOUBLE PRECISION;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS match_datetime TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS date TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS time TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS league TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS country TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS team1 TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS team2 TEXT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS betcount INT;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS ms1 DOUBLE PRECISION;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS ms0 DOUBLE PRECISION;"""))
        conn.execute(text("""ALTER TABLE matches ADD COLUMN IF NOT EXISTS ms2 DOUBLE PRECISION;"""))

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
    payload = nosy_service_call("bettable-matches")  # senin mevcut helper'ın: /service + apiKey
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

            match_id = item.get("MatchID")
            try:
                match_id = int(match_id)
            except Exception:
                skipped += 1
                continue

            # Temel alanlar (bettable-matches response’undan)
            date_val = str(item.get("Date") or "")
            time_val = str(item.get("Time") or "")
            dt_val   = str(item.get("DateTime") or "")
            league   = str(item.get("League") or "")
            country  = str(item.get("Country") or "")
            team1    = str(item.get("Team1") or "")
            team2    = str(item.get("Team2") or "")

            ms1 = item.get("HomeWin")
            ms0 = item.get("Draw")
            ms2 = item.get("AwayWin")
            alt25 = item.get("Under25")
            ust25 = item.get("Over25")
            betcount = item.get("BetCount")

            conn.execute(
                text("""
                    INSERT INTO pool_matches(
                        nosy_match_id, date, time, match_datetime,
                        league, country, team1, team2,
                        ms1, ms0, ms2, alt25, ust25, betcount,
                        fetched_at_tr, raw_json
                    )
                    VALUES(
                        :mid, :date, :time, :dt,
                        :league, :country, :team1, :team2,
                        :ms1, :ms0, :ms2, :alt25, :ust25, :betcount,
                        :fetched_at_tr, :raw_json
                    )
                    ON CONFLICT(nosy_match_id) DO UPDATE SET
                        date          = EXCLUDED.date,
                        time          = EXCLUDED.time,
                        match_datetime= EXCLUDED.match_datetime,
                        league        = EXCLUDED.league,
                        country       = EXCLUDED.country,
                        team1         = EXCLUDED.team1,
                        team2         = EXCLUDED.team2,
                        ms1           = EXCLUDED.ms1,
                        ms0           = EXCLUDED.ms0,
                        ms2           = EXCLUDED.ms2,
                        alt25         = EXCLUDED.alt25,
                        ust25         = EXCLUDED.ust25,
                        betcount      = EXCLUDED.betcount,
                        fetched_at_tr = EXCLUDED.fetched_at_tr,
                        raw_json      = EXCLUDED.raw_json
                """),
                {
                    "mid": match_id,
                    "date": date_val,
                    "time": time_val,
                    "dt": dt_val,
                    "league": league,
                    "country": country,
                    "team1": team1,
                    "team2": team2,
                    "ms1": ms1,
                    "ms0": ms0,
                    "ms2": ms2,
                    "alt25": alt25,
                    "ust25": ust25,
                    "betcount": betcount,
                    "fetched_at_tr": fetched_at_tr,
                    "raw_json": _dump_json(item),
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

# --- Database SYNC ENDPOINT ---
@app.post("/db/matches/sync")
def sync_db_matches():
    """
    pool_matches -> matches
    Havuzdaki maçları filtreleyip matches tablosuna upsert eder.
    """
    fetched_at_tr = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

    selected = 0
    upserted = 0
    skipped = 0

    with engine.begin() as conn:
        # 1) Pool'dan adayları çek (minimum filtre)
        rows = conn.execute(text("""
            SELECT
                nosy_match_id,
                match_datetime, date, time,
                league, country, team1, team2,
                betcount,
                ms1, ms0, ms2, alt25, ust25,
                fetched_at_tr,
                raw_json
            FROM pool_matches
            WHERE
                nosy_match_id IS NOT NULL
                AND ms1 IS NOT NULL AND ms0 IS NOT NULL AND ms2 IS NOT NULL
        """)).mappings().all()

        for r in rows:
            selected += 1

            # Ek (hafif) doğrulama
            try:
                mid = int(r["nosy_match_id"])
            except Exception:
                skipped += 1
                continue

            # İstersen burada daha sıkı filtre koyarsın:
            # - betcount >= X
            # - league whitelist
            # - date/time dolu
            # Şimdilik minimumda bıraktım.

            conn.execute(
                text("""
                    INSERT INTO matches(
                        nosy_match_id,
                        match_datetime, date, time,
                        league, country, team1, team2,
                        betcount,
                        ms1, ms0, ms2, alt25, ust25,
                        source_fetched_at_tr,
                        pool_raw_json,
                        updated_at
                    )
                    VALUES(
                        :mid,
                        :match_datetime, :date, :time,
                        :league, :country, :team1, :team2,
                        :betcount,
                        :ms1, :ms0, :ms2, :alt25, :ust25,
                        :source_fetched_at_tr,
                        :pool_raw_json,
                        NOW()
                    )
                    ON CONFLICT(nosy_match_id) DO UPDATE SET
                        match_datetime        = EXCLUDED.match_datetime,
                        date                 = EXCLUDED.date,
                        time                 = EXCLUDED.time,
                        league               = EXCLUDED.league,
                        country              = EXCLUDED.country,
                        team1                = EXCLUDED.team1,
                        team2                = EXCLUDED.team2,
                        betcount             = EXCLUDED.betcount,
                        ms1                  = EXCLUDED.ms1,
                        ms0                  = EXCLUDED.ms0,
                        ms2                  = EXCLUDED.ms2,
                        alt25                = EXCLUDED.alt25,
                        ust25                = EXCLUDED.ust25,
                        source_fetched_at_tr = EXCLUDED.source_fetched_at_tr,
                        pool_raw_json        = EXCLUDED.pool_raw_json,
                        updated_at           = NOW()
                """),
                {
                    "mid": mid,
                    "match_datetime": r.get("match_datetime") or "",
                    "date": r.get("date") or "",
                    "time": r.get("time") or "",
                    "league": r.get("league") or "",
                    "country": r.get("country") or "",
                    "team1": r.get("team1") or "",
                    "team2": r.get("team2") or "",
                    "betcount": r.get("betcount"),
                    "ms1": r.get("ms1"),
                    "ms0": r.get("ms0"),
                    "ms2": r.get("ms2"),
                    "alt25": r.get("alt25"),
                    "ust25": r.get("ust25"),
                    "source_fetched_at_tr": r.get("fetched_at_tr") or fetched_at_tr,
                    "pool_raw_json": r.get("raw_json") or "",
                }
            )
            upserted += 1

    return {
        "ok": True,
        "selected_from_pool": selected,
        "upserted_into_matches": upserted,
        "skipped": skipped,
        "synced_at_tr": fetched_at_tr
    }
