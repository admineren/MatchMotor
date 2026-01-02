from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

import os
import secrets
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# ------------------------
# Logging
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("matchmotor")

# ------------------------
# ENV
# ------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_USER = os.getenv("ADMIN_USER", "").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "").strip()

NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()
NOSY_BASE_URL = os.getenv("NOSY_BASE_URL", "https://www.nosyapi.com/apiv2/service").strip().rstrip("/")

# Bazı kullanıcılar env'e yanlışlıkla "Bearer xxx" yapıştırabiliyor.
if NOSY_API_KEY.lower().startswith("bearer "):
    NOSY_API_KEY = NOSY_API_KEY.split(" ", 1)[1].strip()

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# ------------------------
# DB
# ------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ------------------------
# App + Auth
# ------------------------
security = HTTPBasic()

app = FastAPI(
    title="MatchMotor API",
    version="2.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

def authenticate(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    if not ADMIN_USER or not ADMIN_PASS:
        raise HTTPException(status_code=500, detail="ADMIN_USER / ADMIN_PASS env missing")

    ok_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Unauthorized")

    return credentials.username

# ------------------------
# Helpers
# ------------------------
TR_TZ = ZoneInfo("Europe/Istanbul")

def tr_now() -> datetime:
    return datetime.now(TR_TZ)

def parse_day(day: Optional[str]) -> Optional[date]:
    if not day:
        return None
    try:
        return datetime.strptime(day, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")

def to_dt(date_str: str, time_str: str) -> datetime:
    # Nosy: "2024-01-31" + "19:00:00"
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

def nosy_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env missing")

    url = f"{NOSY_BASE_URL}/{path.lstrip('/')}"
    params = params or {}

    # 1) Authorization: Bearer <key>  (senin curl örneğin gibi)
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {NOSY_API_KEY}"}, params=params, timeout=30)
        if r.status_code != 401:
            r.raise_for_status()
            return r.json()
    except requests.RequestException:
        pass

    # 2) X-NSYP: <key>  (bazı Nosy servislerinde çalışıyor)
    try:
        r = requests.get(url, headers={"X-NSYP": NOSY_API_KEY}, params=params, timeout=30)
        if r.status_code != 401:
            r.raise_for_status()
            return r.json()
    except requests.RequestException:
        pass

    # 3) Query param fallback: ?apiKey=<key>  (en “inatçı” çözüm)
    try:
        params2 = dict(params)
        params2["apiKey"] = NOSY_API_KEY
        r = requests.get(url, params=params2, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI request failed: {e}")

# ------------------------
# Schema bootstrap
# ------------------------
@app.on_event("startup")
def ensure_schema():
    """
    Telefonla SQL çalıştıramadığın için: tabloları app açılışında garanti ediyoruz.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS matches (
              id SERIAL PRIMARY KEY,
              match_id INTEGER UNIQUE NOT NULL,
              datetime TIMESTAMP NOT NULL,
              country TEXT,
              league TEXT,
              team1 TEXT,
              team2 TEXT,
              home_win DOUBLE PRECISION,
              draw DOUBLE PRECISION,
              away_win DOUBLE PRECISION,
              under25 DOUBLE PRECISION,
              over25 DOUBLE PRECISION,
              bet_count INTEGER,
              created_at TIMESTAMP DEFAULT NOW(),
              updated_at TIMESTAMP DEFAULT NOW()
            );
        """))
        
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league);"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS match_results (
              id SERIAL PRIMARY KEY,
              match_id INTEGER UNIQUE NOT NULL,
              ms_home SMALLINT,
              ms_away SMALLINT,
              ht_home SMALLINT,
              ht_away SMALLINT,
              home_corner SMALLINT,
              away_corner SMALLINT,
              home_yellow SMALLINT,
              away_yellow SMALLINT,
              home_red SMALLINT,
              away_red SMALLINT,
              created_at TIMESTAMP DEFAULT NOW(),
              updated_at TIMESTAMP DEFAULT NOW()
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_results_match_id ON match_results(match_id);"))

    logger.info("Schema ensured.")

# ------------------------
# Routes
# ------------------------
@app.get("/health")
def health():
    return {"ok": True, "time_tr": tr_now().isoformat()}

@app.get("/matches")
def list_matches(
    limit: int = Query(50, ge=1, le=500),
    day: Optional[str] = Query(None, description="YYYY-MM-DD (optional)"),
    user: str = Depends(authenticate),
    db: Session = Depends(get_db),
):
    d = parse_day(day)

    where = ""
    params: Dict[str, Any] = {"lim": limit}
    if d:
        where = "WHERE DATE(m.datetime) = :d"
        params["d"] = d

    # total
    total_sql = f"SELECT COUNT(*) FROM matches m {where}"
    total = int(db.execute(text(total_sql), params).scalar() or 0)

    # join view
    sql = f"""
      SELECT
        m.match_id,
        m.datetime,
        m.country,
        m.league,
        m.team1,
        m.team2,
        m.home_win, m.draw, m.away_win,
        m.under25, m.over25,
        m.bet_count,
        r.ms_home, r.ms_away, r.ht_home, r.ht_away,
        r.home_corner, r.away_corner,
        r.home_yellow, r.away_yellow,
        r.home_red, r.away_red
      FROM matches m
      LEFT JOIN match_results r ON r.match_id = m.match_id
      {where}
      ORDER BY m.datetime DESC
      LIMIT :lim
    """
    rows = db.execute(text(sql), params).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        dt = r["datetime"]
        out.append({
            "MatchID": r["match_id"],
            "Tarih": dt.strftime("%Y-%m-%d"),
            "Saat": dt.strftime("%H:%M"),
            "Ulke": r["country"],
            "Lig": r["league"],
            "Ev": r["team1"],
            "Deplasman": r["team2"],

            "MS1": r["home_win"],
            "MS0": r["draw"],
            "MS2": r["away_win"],
            "Alt25": r["under25"],
            "Ust25": r["over25"],

            "MS_Ev": r["ms_home"],
            "MS_Dep": r["ms_away"],
            "IY_Ev": r["ht_home"],
            "IY_Dep": r["ht_away"],

            "Korner_Ev": r["home_corner"],
            "Korner_Dep": r["away_corner"],
            "Sari_Ev": r["home_yellow"],
            "Sari_Dep": r["away_yellow"],
            "Kirmizi_Ev": r["home_red"],
            "Kirmizi_Dep": r["away_red"],

            "BetCount": r["bet_count"],
        })

    return {"total": total, "returned": len(out), "matches": out}

@app.get("/daily-summary")
def daily_summary(
    day: Optional[str] = Query(None, description="YYYY-MM-DD (default=today TR)"),
    user: str = Depends(authenticate),
    db: Session = Depends(get_db),
):
    d = parse_day(day) or tr_now().date()

    total = int(db.execute(
        text("SELECT COUNT(*) FROM matches WHERE DATE(datetime) = :d"),
        {"d": d}
    ).scalar() or 0)

    odds_ok = int(db.execute(
        text("""
            SELECT COUNT(*) FROM matches
            WHERE DATE(datetime)=:d
              AND home_win IS NOT NULL AND draw IS NOT NULL AND away_win IS NOT NULL
        """),
        {"d": d}
    ).scalar() or 0)

    results_ok = int(db.execute(
        text("""
            SELECT COUNT(*) FROM match_results r
            JOIN matches m ON m.match_id = r.match_id
            WHERE DATE(m.datetime)=:d
              AND r.ms_home IS NOT NULL AND r.ms_away IS NOT NULL
        """),
        {"d": d}
    ).scalar() or 0)

    return {
        "day_tr": d.isoformat(),
        "total_matches": total,
        "matches_with_odds_1x2": odds_ok,
        "matches_with_results": results_ok
    }

# ------------------------
# SYNC: BETTABLE (odds)
# ------------------------
@app.post("/sync/bettable")
def sync_bettable(
    day: Optional[str] = Query(None, description="Optional YYYY-MM-DD. (If Nosy supports it, we pass as 'date')"),
    user: str = Depends(authenticate),
    db: Session = Depends(get_db),
):
    """
    NosyAPI -> bettable-matches/opening-odds
    DB -> matches (upsert by match_id)
    """
    d = parse_day(day)
    params: Dict[str, Any] = {}
    if d:
        # Nosy param adı dokümana göre değişebilir; varsa çalışır, yoksa görmezden gelir.
        params["date"] = d.isoformat()

    payload = nosy_get("bettable-matches/opening-odds", params=params)

    if payload.get("status") != "success":
        raise HTTPException(status_code=502, detail=f"NosyAPI returned non-success: {payload}")

    items = payload.get("data") or []
    upsert_sql = text("""
        INSERT INTO matches (
          match_id, datetime, country, league, team1, team2,
          home_win, draw, away_win, under25, over25, bet_count, updated_at
        )
        VALUES (
          :match_id, :dt, :country, :league, :team1, :team2,
          :home_win, :draw, :away_win, :under25, :over25, :bet_count, NOW()
        )
        ON CONFLICT (match_id) DO UPDATE SET
          datetime = EXCLUDED.datetime,
          country  = EXCLUDED.country,
          league   = EXCLUDED.league,
          team1    = EXCLUDED.team1,
          team2    = EXCLUDED.team2,
          home_win = EXCLUDED.home_win,
          draw     = EXCLUDED.draw,
          away_win = EXCLUDED.away_win,
          under25  = EXCLUDED.under25,
          over25   = EXCLUDED.over25,
          bet_count= EXCLUDED.bet_count,
          updated_at = NOW()
    """)

    written = 0
    with db.begin():
        for it in items:
            try:
                match_id = int(it.get("MatchID"))
                dt = to_dt(it.get("Date"), it.get("Time"))
                db.execute(upsert_sql, {
                    "match_id": match_id,
                    "dt": dt,
                    "country": it.get("Country"),
                    "league": it.get("League"),
                    "team1": it.get("Team1"),
                    "team2": it.get("Team2"),
                    "home_win": it.get("HomeWin"),
                    "draw": it.get("Draw"),
                    "away_win": it.get("AwayWin"),
                    "under25": it.get("Under25"),
                    "over25": it.get("Over25"),
                    "bet_count": it.get("BetCount"),
                })
                written += 1
            except Exception:
                # tek bir bozuk satır yüzünden tüm sync ölmesin
                logger.exception("Skipping bad bettable row: %s", it)

    return {
        "ok": True,
        "source_rowCount": payload.get("rowCount"),
        "written": written,
        "creditUsed": payload.get("creditUsed"),
    }

# ------------------------
# SYNC: RESULTS (scores)
# ------------------------
@app.post("/sync/results")
def sync_results(
    day: Optional[str] = Query(None, description="Optional YYYY-MM-DD. (If Nosy supports it, we pass as 'date')"),
    user: str = Depends(authenticate),
    db: Session = Depends(get_db),
):
    """
    NosyAPI -> matches-result
    DB -> match_results (upsert by match_id)
    """
    d = parse_day(day)
    params: Dict[str, Any] = {}
    if d:
        params["date"] = d.isoformat()

    payload = nosy_get("matches-result", params=params)

    if payload.get("status") != "success":
        raise HTTPException(status_code=502, detail=f"NosyAPI returned non-success: {payload}")

    items = payload.get("data") or []

    upsert_sql = text("""
        INSERT INTO match_results (
          match_id,
          ms_home, ms_away, ht_home, ht_away,
          home_corner, away_corner,
          home_yellow, away_yellow,
          home_red, away_red,
          updated_at
        )
        VALUES (
          :match_id,
          :ms_home, :ms_away, :ht_home, :ht_away,
          :home_corner, :away_corner,
          :home_yellow, :away_yellow,
          :home_red, :away_red,
          NOW()
        )
        ON CONFLICT (match_id) DO UPDATE SET
          ms_home = EXCLUDED.ms_home,
          ms_away = EXCLUDED.ms_away,
          ht_home = EXCLUDED.ht_home,
          ht_away = EXCLUDED.ht_away,
          home_corner = EXCLUDED.home_corner,
          away_corner = EXCLUDED.away_corner,
          home_yellow = EXCLUDED.home_yellow,
          away_yellow = EXCLUDED.away_yellow,
          home_red = EXCLUDED.home_red,
          away_red = EXCLUDED.away_red,
          updated_at = NOW()
    """)

    def parse_meta(meta_list: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
        m: Dict[str, Optional[int]] = {
            "msHomeScore": None, "msAwayScore": None,
            "htHomeScore": None, "htAwayScore": None,
            "homeCorner": None, "awayCorner": None,
            "homeyellowCard": None, "awayyellowCard": None,
            "homeredCard": None, "awayredCard": None,
        }
        for x in meta_list or []:
            k = x.get("metaName")
            v = x.get("value")
            if k in m:
                try:
                    m[k] = int(v) if v is not None else None
                except Exception:
                    m[k] = None
        return m

    written = 0
    with db.begin():
        for it in items:
            try:
                match_id = int(it.get("MatchID"))
                meta = parse_meta(it.get("matchResult") or [])

                db.execute(upsert_sql, {
                    "match_id": match_id,
                    "ms_home": meta["msHomeScore"],
                    "ms_away": meta["msAwayScore"],
                    "ht_home": meta["htHomeScore"],
                    "ht_away": meta["htAwayScore"],
                    "home_corner": meta["homeCorner"],
                    "away_corner": meta["awayCorner"],
                    "home_yellow": meta["homeyellowCard"],
                    "away_yellow": meta["awayyellowCard"],
                    "home_red": meta["homeredCard"],
                    "away_red": meta["awayredCard"],
                })
                written += 1
            except Exception:
                logger.exception("Skipping bad result row: %s", it)

    return {
        "ok": True,
        "source_rowCount": payload.get("rowCount"),
        "written": written,
        "creditUsed": payload.get("creditUsed"),
    }

# ------------------------
# Protected OpenAPI + Docs
# ------------------------
@app.get("/openapi.json")
def openapi_json(user: str = Depends(authenticate)):
    return JSONResponse(
        get_openapi(title=app.title, version=app.version, routes=app.routes)
    )

@app.get("/docs", response_class=HTMLResponse)
def docs(user: str = Depends(authenticate)):
    return get_swagger_ui_html(openapi_url="/openapi.json", title="MatchMotor API - Docs")
