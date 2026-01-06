import os
import requests
import json
import datetime as dt
import time

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

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

def _to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

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

# -----------------------------
# FINISHED MATCHES (matches-result snapshot)
# -----------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finished_matches (
                id BIGSERIAL PRIMARY KEY,
                nosy_match_id BIGINT NOT NULL UNIQUE,

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
                        fetched_at_tr, raw_json, game_result
                    )
                    VALUES(
                        :mid, :date, :time, :dt,
                        :league, :country, :team1, :team2,
                        :ms1, :ms0, :ms2, :alt25, :ust25, :betcount,
                        :fetched_at_tr, :raw_json,
                        :game_result
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
                        raw_json      = EXCLUDED.raw_json,
                        game_result   = EXCLUDED.game_result
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
                    "game_result": item.get("GameResult"),
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

    return {
        "ok": True,
        "selected_from_pool": selected,
        "upserted_into_matches": upserted,
        "skipped": skipped,
        "synced_at_tr": fetched_at_tr
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
    - Bulk: API'nin döndürdüğü biten maçları finished_matches tablosuna upsert eder.
    - Backfill: Sadece 'dün 22:00-23:59' arası başlayan ve finished'a geçmemiş maçlar için details dener.
    """

    # küçük/büyük harf farkını öldüren meta parser
    def _meta_map_ci(match_result):
        base = _meta_map(match_result)
        if not isinstance(base, dict):
            return {}
        # key'leri normalize et (lower)
        out = {}
        for k, v in base.items():
            if isinstance(k, str):
                out[k.lower()] = v
        return out

    # -----------------------
    # 1) BULK: matches-result
    # -----------------------
    payload = nosy_service_call("matches-result")
    data = payload.get("data") or []
    received = len(data)

    fetched_at_tr = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

    upserted = 0
    skipped = 0
    no_score = 0

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

            meta = _meta_map_ci(item.get("matchResult"))

            # NOTE: metaName'ler API'de bazen farklı casing ile gelebiliyor, o yüzden lower map kullanıyoruz
            ft_home = _to_int(meta.get("mshomescore"))
            ft_away = _to_int(meta.get("msawayscore"))
            ht_home = _to_int(meta.get("hthomescore"))
            ht_away = _to_int(meta.get("htawayscore"))

            home_corner = _to_int(meta.get("homecorner"))
            away_corner = _to_int(meta.get("awaycorner"))
            home_yellow = _to_int(meta.get("homeyellowcard"))
            away_yellow = _to_int(meta.get("awayyellowcard"))
            home_red = _to_int(meta.get("homeredcard"))
            away_red = _to_int(meta.get("awayredcard"))

            # skor yoksa finished sayma
            if ft_home is None or ft_away is None:
                no_score += 1
                continue

            conn.execute(
                text(
                    """
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
                    """
                ),
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
                    "result": item.get("Result"),
                    "game_result": item.get("GameResult"),
                    "live_status": item.get("LiveStatus"),
                    "fetched_at_tr": fetched_at_tr,
                    "raw_json": _dump_json(item),
                },
            )
            upserted += 1

        # -----------------------
        # 2) BACKFILL (optional)
        # -----------------------
        backfill_report = {
            "enabled": bool(backfill),
            "window": None,
            "candidates": 0,
            "requested": 0,
            "upserted": 0,
            "no_score": 0,
            "skipped": 0,
        }

        if int(backfill) == 1 and int(max_details) > 0:
            now_tr = datetime.now(TR_TZ) if TR_TZ else datetime.utcnow()
            yesterday = (now_tr.date() - timedelta(days=1)).isoformat()

            # "dün 22:00:00 - 23:59:59" bandı
            t_from = "22:00:00"
            t_to = "23:59:59"

            backfill_report["window"] = {"day": yesterday, "time_from": t_from, "time_to": t_to}

            mids = (
                conn.execute(
                    text(
                        """
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
                        """
                    ),
                    {"day": yesterday, "t_from": t_from, "t_to": t_to, "lim": int(max_details)},
                )
                .scalars()
                .all()
            )

            backfill_report["candidates"] = len(mids)

            for mid in mids:
                backfill_report["requested"] += 1

                # Swagger ekranında param adı matchID görünüyor.
                # Senin wrapper bunu match_id ile map ediyorsa da sorun olmasın diye iki anahtarı da veriyoruz.
                details = nosy_service_call(
                    "matches-result/details",
                    params={"matchID": int(mid), "match_id": int(mid)},
                )
                dd = (details or {}).get("data")

                item = None
                if isinstance(dd, list) and dd:
                    item = dd[0]
                elif isinstance(dd, dict):
                    item = dd

                if not isinstance(item, dict):
                    backfill_report["skipped"] += 1
                    continue

                meta = _meta_map_ci(item.get("matchResult"))

                ft_home = _to_int(meta.get("mshomescore"))
                ft_away = _to_int(meta.get("msawayscore"))
                ht_home = _to_int(meta.get("hthomescore"))
                ht_away = _to_int(meta.get("htawayscore"))

                home_corner = _to_int(meta.get("homecorner"))
                away_corner = _to_int(meta.get("awaycorner"))
                home_yellow = _to_int(meta.get("homeyellowcard"))
                away_yellow = _to_int(meta.get("awayyellowcard"))
                home_red = _to_int(meta.get("homeredcard"))
                away_red = _to_int(meta.get("awayredcard"))

                if ft_home is None or ft_away is None:
                    backfill_report["no_score"] += 1
                    continue

                # backfill yazarken de "details item" içinden alanları alıyoruz (casing PascalCase)
                fetched_at_tr_bf = datetime.now(TR_TZ).isoformat() if TR_TZ else datetime.utcnow().isoformat()

                conn.execute(
                    text(
                        """
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
                        """
                    ),
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
                        "result": item.get("Result"),
                        "game_result": item.get("GameResult"),
                        "live_status": item.get("LiveStatus"),
                        "fetched_at_tr": fetched_at_tr_bf,
                        "raw_json": _dump_json(item),
                    },
                )

                backfill_report["upserted"] += 1

    return {
        "ok": True,
        "endpoint": "matches-result",
        "bulk": {
            "received": received,
            "upserted": upserted,
            "skipped": skipped,
            "no_score": no_score,
            "fetched_at_tr": fetched_at_tr,
            "rowCount": payload.get("rowCount"),
            "creditUsed": payload.get("creditUsed"),
        },
        "backfill": backfill_report,
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

# ---- FreeAPI config
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
FREEAPI_HOST = os.getenv("FREEAPI_HOST", "free-api-live-football-data.p.rapidapi.com").strip()

# ---- Plan limit (senin ücretsiz plan: 100 / month)
FREEAPI_MONTHLY_LIMIT = int(os.getenv("FREEAPI_MONTHLY_LIMIT", "100"))

# ---- In-memory usage (restart olunca sıfırlanır)
FREEAPI_USED_THIS_MONTH = 0

def _require_rapidapi_key():
    if not RAPIDAPI_KEY:
        raise HTTPException(status_code=500, detail="RAPIDAPI_KEY env eksik.")

def _now_month_key() -> str:
    # "2026-01" gibi
    return datetime.now(timezone.utc).strftime("%Y-%m")

def freeapi_get_with_meta(path: str, params: dict | None = None) -> tuple[dict, dict]:
    """
    JSON + meta döner:
    - payload bytes (gerçek)
    - elapsed ms
    - status code
    - content-length header (varsa)
    """
    _require_rapidapi_key()

    base = f"https://{FREEAPI_HOST}".rstrip("/")
    url = f"{base}/{path.lstrip('/')}"

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": FREEAPI_HOST,
    }

    t0 = time.time()
    try:
        r = requests.get(url, headers=headers, params=(params or {}), timeout=60)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"FreeAPI bağlantı hatası: {e}")
    elapsed_ms = int((time.time() - t0) * 1000)

    payload_bytes = len(r.content or b"")
    header_content_length = r.headers.get("content-length")

    if r.status_code != 200:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise HTTPException(
            status_code=r.status_code,
            detail={
                "url": str(r.url),
                "elapsed_ms": elapsed_ms,
                "payload_bytes": payload_bytes,
                "content_length_header": header_content_length,
                "body": body,
            },
        )

    try:
        data = r.json()
    except Exception:
        raise HTTPException(
            status_code=502,
            detail={"url": str(r.url), "elapsed_ms": elapsed_ms, "body": r.text},
        )

    meta = {
        "url": str(r.url),
        "elapsed_ms": elapsed_ms,
        "payload_bytes": payload_bytes,
        "payload_kb": round(payload_bytes / 1024, 2),
        "payload_mb": round(payload_bytes / (1024 * 1024), 3),
        "content_length_header": header_content_length,
    }
    return data, meta


@app.get("/freeapi/test/ping", tags=["FreeAPI"])
def freeapi_test_ping():
    """
    Upstream request harcamaz. Sadece env/host + bizim sayaç.
    """
    global FREEAPI_USED_THIS_MONTH

    used = FREEAPI_USED_THIS_MONTH
    remaining = max(FREEAPI_MONTHLY_LIMIT - used, 0)

    return {
        "ok": True,
        "rapidapi_key_set": bool(RAPIDAPI_KEY),
        "freeapi_host": FREEAPI_HOST,
        "month_key_utc": _now_month_key(),
        "plan_monthly_limit": FREEAPI_MONTHLY_LIMIT,
        "used_by_app_counter": used,
        "remaining_estimated_by_app_counter": remaining,
        "note": "Bu kalan/used değeri uygulama sayacıdır; restart/deploy olursa sıfırlanır.",
    }


@app.get("/freeapi/test/matches-by-date/full", tags=["FreeAPI"])
def freeapi_matches_by_date_full(
    date: str = Query(..., description="YYYYMMDD örn 20260104"),
    include_matches: bool = Query(True, description="true ise tüm maç listesini döndürür (büyük JSON!)"),
):
    """
    1 upstream request:
    - match_count
    - payload KB/MB
    - elapsed
    - (opsiyonel) tüm matches
    """
    global FREEAPI_USED_THIS_MONTH

    data, meta = freeapi_get_with_meta("football-get-matches-by-date", params={"date": date})
    FREEAPI_USED_THIS_MONTH += 1

    response = data.get("response") if isinstance(data, dict) else None
    matches = (response or {}).get("matches") if isinstance(response, dict) else None
    match_count = len(matches) if isinstance(matches, list) else None

    used = FREEAPI_USED_THIS_MONTH
    remaining = max(FREEAPI_MONTHLY_LIMIT - used, 0)

    out = {
        "ok": True,
        "requested_date": date,
        "host": FREEAPI_HOST,
        "match_count": match_count,
        "meta": meta,
        "usage": {
            "month_key_utc": _now_month_key(),
            "plan_monthly_limit": FREEAPI_MONTHLY_LIMIT,
            "used_by_app_counter": used,
            "remaining_estimated_by_app_counter": remaining,
        },
        "response_keys": list(response.keys()) if isinstance(response, dict) else None,
    }

    if include_matches:
        out["matches"] = matches  # FULL LIST

    return out
    
