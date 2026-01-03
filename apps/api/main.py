import os
import json
from typing import Optional, Dict, Any

import requests
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./matches.db")

NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()
NOSY_BASE_URL = os.getenv("NOSY_BASE_URL", "https://www.nosyapi.com/apiv2").strip()

# Ayrı apiID'ler
NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID", "1881134").strip()      # odds / matches api
NOSY_RESULTS_API_ID = os.getenv("NOSY_RESULTS_API_ID", "1881149").strip()  # results api

# Basit admin koruması (istersen kullan)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

# ------------------------------------------------------------
# APP + DB
# ------------------------------------------------------------
app = FastAPI(title="MatchMotor API - Docs", version="0.1.0")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _safe_exec(conn, sql: str):
    """Startup'ta DB yüzünden uygulama çökmesin diye."""
    try:
        conn.execute(text(sql))
    except Exception as e:
        # sadece log gibi davranalım; render loglarında görürsün
        print(f"[WARN] SQL failed: {sql} -> {e}")


@app.on_event("startup")
def startup():
    # Basit şema (senin yapına göre)
    with engine.begin() as conn:
        _safe_exec(
            conn,
            """
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY,
                date TEXT,
                time TEXT,
                datetime TEXT,
                league TEXT,
                country TEXT,
                teams TEXT,
                team1 TEXT,
                team2 TEXT,
                live_status INTEGER,
                raw_json TEXT
            );
            """,
        )

        _safe_exec(
            conn,
            """
            CREATE TABLE IF NOT EXISTS match_odds (
                match_id INTEGER PRIMARY KEY,
                opening_odds_json TEXT
            );
            """,
        )

        _safe_exec(
            conn,
            """
            CREATE TABLE IF NOT EXISTS match_results (
                match_id INTEGER PRIMARY KEY,
                result_json TEXT
            );
            """,
        )

        # Indexler (hata verirse uygulama kapanmasın)
        _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league);")
        _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_results_match_id ON match_results(match_id);")


# ------------------------------------------------------------
# NOSY HELPERS
# ------------------------------------------------------------
def _normalize_nosy_base(base: str) -> str:
    """
    NOSY_BASE_URL:
      - https://www.nosyapi.com/apiv2        -> https://www.nosyapi.com/apiv2/service
      - https://www.nosyapi.com/apiv2/service -> aynı kalır
    """
    b = base.rstrip("/")
    if b.endswith("/service"):
        return b
    return b + "/service"


def nosy_get(path: str, api_id: Optional[str] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not NOSY_API_KEY:
        raise HTTPException(status_code=500, detail="NOSY_API_KEY env boş.")

    base = _normalize_nosy_base(NOSY_BASE_URL)
    url = f"{base}/{path.lstrip('/')}"

    q = dict(params or {})
    q["apiKey"] = NOSY_API_KEY
    if api_id:
        q["apiID"] = api_id

    try:
        r = requests.get(url, params=q, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        # Nosyapi çoğu zaman 404/401'i burada verir
        raise HTTPException(status_code=502, detail=f"NosyAPI HTTP error: {e} for url: {r.url}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI request failed: {e}")


# ------------------------------------------------------------
# BASIC ENDPOINTS
# ------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/matches")
def list_matches(limit: int = 50, db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT match_id, datetime, league, teams, live_status FROM matches ORDER BY datetime DESC LIMIT :l"),
        {"l": limit},
    ).mappings().all()
    return {"count": len(rows), "data": list(rows)}


@app.post("/admin/matches/clear")
def clear_matches(x_admin_token: Optional[str] = Header(default=None), db: Session = Depends(get_db)):
    if ADMIN_TOKEN and x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized (ADMIN_TOKEN).")

    db.execute(text("DELETE FROM match_results"))
    db.execute(text("DELETE FROM match_odds"))
    db.execute(text("DELETE FROM matches"))
    db.commit()
    return {"ok": True, "message": "Cleared."}


# ------------------------------------------------------------
# NOSY ENDPOINTS
# ------------------------------------------------------------
@app.get("/nosy-check")
def nosy_check():
    """
    Nosy servis durumu / API key kontrolü.
    Not: check endpointi bazı paketlerde farklı davranabiliyor.
    Bu yüzden önce RESULTS apiID ile dener, olmazsa ODDS apiID ile dener.
    """
    last_err = None

    for api_id in [NOSY_RESULTS_API_ID, NOSY_ODDS_API_ID]:
        try:
            return nosy_get("/nosy-service/check", api_id=api_id)
        except HTTPException as e:
            last_err = e.detail

    raise HTTPException(status_code=502, detail=f"Nosy check failed for both apiIDs. Last error: {last_err}")


@app.get("/nosy-matches-by-date")
def nosy_matches_by_date(date: str = Query(..., description="YYYY-MM-DD"), db: Session = Depends(get_db)):
    """
    Endpoint: bettable-matches/date
    Bu endpointten gelen maçları DB'ye upsert ederiz.
    """
    payload = nosy_get("/bettable-matches/date", api_id=NOSY_ODDS_API_ID, params={"date": date})

    # beklenen format: {"status": "...", "data": [ {MatchID...}, ... ] } gibi
    data = payload.get("data") or []
    upserted = 0

    for item in data:
        # Nosy bazen MatchID, bazen match_id döndürebilir
        match_id = item.get("MatchID") or item.get("match_id") or item.get("matchId")
        if not match_id:
            continue

        dt = item.get("DateTime") or item.get("datetime") or item.get("Date")  # yedek
        league = item.get("League")
        country = item.get("Country")
        teams = item.get("Teams")
        team1 = item.get("Team1")
        team2 = item.get("Team2")
        live_status = item.get("LiveStatus")

        db.execute(
            text(
                """
                INSERT INTO matches (match_id, date, time, datetime, league, country, teams, team1, team2, live_status, raw_json)
                VALUES (:match_id, :date, :time, :datetime, :league, :country, :teams, :team1, :team2, :live_status, :raw_json)
                ON CONFLICT(match_id) DO UPDATE SET
                    datetime=excluded.datetime,
                    league=excluded.league,
                    country=excluded.country,
                    teams=excluded.teams,
                    team1=excluded.team1,
                    team2=excluded.team2,
                    live_status=excluded.live_status,
                    raw_json=excluded.raw_json
                """
            ),
            {
                "match_id": int(match_id),
                "date": item.get("Date"),
                "time": item.get("Time"),
                "datetime": dt,
                "league": league,
                "country": country,
                "teams": teams,
                "team1": team1,
                "team2": team2,
                "live_status": live_status if live_status is None else int(live_status),
                "raw_json": json.dumps(item, ensure_ascii=False),
            },
        )
        upserted += 1

    db.commit()
    return {"ok": True, "date": date, "upserted": upserted, "nosy": payload.get("status") or payload.get("message")}


@app.get("/nosy-opening-odds")
def nosy_opening_odds(match_id: int = Query(..., description="Nosy MatchID (ör: 151738)"), db: Session = Depends(get_db)):
    """
    Endpoint: bettable-matches/opening-odds
    """
    payload = nosy_get("/bettable-matches/opening-odds", api_id=NOSY_ODDS_API_ID, params={"match_id": match_id})

    # DB'ye yaz
    db.execute(
        text(
            """
            INSERT INTO match_odds (match_id, opening_odds_json)
            VALUES (:match_id, :j)
            ON CONFLICT(match_id) DO UPDATE SET opening_odds_json=excluded.opening_odds_json
            """
        ),
        {"match_id": match_id, "j": json.dumps(payload, ensure_ascii=False)},
    )
    db.commit()
    return payload


@app.get("/nosy-result-details")
def nosy_result_details(match_id: int = Query(..., description="Nosy MatchID (ör: 151738)"), db: Session = Depends(get_db)):
    """
    Endpoint: bettable-result/details
    """
    payload = nosy_get("/bettable-result/details", api_id=NOSY_RESULTS_API_ID, params={"match_id": match_id})

    db.execute(
        text(
            """
            INSERT INTO match_results (match_id, result_json)
            VALUES (:match_id, :j)
            ON CONFLICT(match_id) DO UPDATE SET result_json=excluded.result_json
            """
        ),
        {"match_id": match_id, "j": json.dumps(payload, ensure_ascii=False)},
    )
    db.commit()
    return payload
