import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List

import requests
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.status import HTTP_401_UNAUTHORIZED

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Float,
    Text,
    text,
)
from sqlalchemy.orm import sessionmaker, declarative_base, Session


# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("matchmotor")


# -----------------------------
# ENV
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin").strip()

NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()
NOSY_BASE_URL = os.getenv("NOSY_BASE_URL", "https://www.nosyapi.com/apiv2/service").strip()

NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID", "").strip()      # örn: 1881134
NOSY_RESULTS_API_ID = os.getenv("NOSY_RESULTS_API_ID", "").strip()  # örn: 1881149

# Bazı kullanıcılar yanlışlıkla "Bearer xxx" yapıştırabiliyor:
if NOSY_API_KEY.lower().startswith("bearer "):
    NOSY_API_KEY = NOSY_API_KEY.split(" ", 1)[1].strip()

# base url normalize
NOSY_BASE_URL = NOSY_BASE_URL.rstrip("/")
# kullanıcı bazen https://www.nosyapi.com/apiv2 yazarsa düzeltelim:
if NOSY_BASE_URL.endswith("/apiv2"):
    NOSY_BASE_URL = NOSY_BASE_URL + "/service"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

if not NOSY_API_KEY:
    logger.warning("NOSY_API_KEY is empty. Nosy endpoints will fail until you set it.")

if not NOSY_ODDS_API_ID:
    logger.warning("NOSY_ODDS_API_ID is empty. Odds endpoints will fail until you set it.")

if not NOSY_RESULTS_API_ID:
    logger.warning("NOSY_RESULTS_API_ID is empty. Results endpoints will fail until you set it.")


# -----------------------------
# DB
# -----------------------------
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True, index=True)
    match_id = Column(Integer, unique=True, index=True, nullable=False)  # Nosy MatchID
    datetime = Column(DateTime, index=True, nullable=True)

    league = Column(String(255), index=True, nullable=True)
    country = Column(String(255), nullable=True)

    team1 = Column(String(255), nullable=True)
    team2 = Column(String(255), nullable=True)

    home_win = Column(Float, nullable=True)
    draw = Column(Float, nullable=True)
    away_win = Column(Float, nullable=True)

    under25 = Column(Float, nullable=True)
    over25 = Column(Float, nullable=True)

    raw_json = Column(Text, nullable=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=engine)

    # Indexler (idempotent). Postgres'te IF NOT EXISTS destekli.
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league);"))


# -----------------------------
# AUTH
# -----------------------------
security = HTTPBasic()


def require_admin(credentials: HTTPBasicCredentials = Depends(security)):
    ok = (credentials.username == ADMIN_USER and credentials.password == ADMIN_PASS)
    if not ok:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


# -----------------------------
# NOSY HELPERS
# -----------------------------
class NosyError(Exception):
    pass


def nosy_request(api_id: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Nosy formatı:  GET {BASE}/{endpoint}?apiKey=...&apiID=...&...
    endpoint örn: "nosy-service/check" veya "bettable-matches/opening-odds"
    """
    if not api_id:
        raise NosyError("api_id is empty")
    if not NOSY_API_KEY:
        raise NosyError("NOSY_API_KEY is empty")

    endpoint = endpoint.lstrip("/")
    url = f"{NOSY_BASE_URL}/{endpoint}"

    q = {}
    if params:
        q.update(params)

    # Nosy dokümanındaki query paramlar:
    q["apiKey"] = NOSY_API_KEY
    q["apiID"] = api_id

    logger.info(f"Nosy GET {url} params={ {k:v for k,v in q.items() if k.lower()!='apikey'} }")

    r = requests.get(url, params=q, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Bazı endpointler status=failure dönebiliyor ama HTTP 200
    return data


def nosy_request_with_matchid_fallback(api_id: str, endpoint: str, match_id: int) -> Dict[str, Any]:
    """
    Bazı Nosy endpointleri match param adını farklı bekliyor:
    - matchID
    - MatchID
    - matchId (nadiren)
    Bu yüzden sırayla deneriz.
    """
    tried = []
    for key in ("matchID", "MatchID", "matchId", "match_id"):
        tried.append(key)
        data = nosy_request(api_id, endpoint, params={key: match_id})
        msg = (data.get("message") or "").lower()
        msgtr = (data.get("messageTR") or "").lower()

        # "Match ID not found" geldiyse diğer key'i dene
        if "match id not found" in msg or "match id bulunamadı" in msgtr:
            continue
        return data

    # Hepsi aynı hatayı verdiyse son datayı döndür
    return data  # type: ignore


# -----------------------------
# APP
# -----------------------------
app = FastAPI(openapi_url="/openapi.json", title="MatchMotor API - Docs")


@app.on_event("startup")
def on_startup():
    init_db()
    logger.info("DB initialized")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/matches")
def list_matches(limit: int = 50, db: Session = Depends(get_db)):
    rows: List[Match] = db.query(Match).order_by(Match.datetime.desc().nullslast()).limit(limit).all()
    return [
        {
            "match_id": r.match_id,
            "datetime": r.datetime.isoformat() if r.datetime else None,
            "league": r.league,
            "country": r.country,
            "team1": r.team1,
            "team2": r.team2,
            "home_win": r.home_win,
            "draw": r.draw,
            "away_win": r.away_win,
            "under25": r.under25,
            "over25": r.over25,
        }
        for r in rows
    ]


@app.post("/admin/matches/clear")
def clear_matches(_: bool = Depends(require_admin), db: Session = Depends(get_db)):
    db.query(Match).delete()
    db.commit()
    return {"ok": True}


# -----------------------------
# NOSY ENDPOINTS
# -----------------------------
@app.get("/nosy-check")
def nosy_check():
    """
    Nosy servis durumu / API key kontrolü.
    Dokümandaki endpoint: /nosy-service/check (service altında).
    """
    try:
        return nosy_request(NOSY_ODDS_API_ID or NOSY_RESULTS_API_ID, "nosy-service/check")
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI HTTP error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NosyAPI error: {e}")


@app.get("/nosy-opening-odds")
def nosy_opening_odds(
    match_id: int = Query(..., description="Nosy MatchID (ör: 151738)"),
):
    """
    Açılış oranları:
    Nosy endpoint: /bettable-matches/opening-odds
    ÖNEMLİ: Parametre adı Nosy tarafında genelde MatchID / matchID.
    """
    try:
        return nosy_request_with_matchid_fallback(NOSY_ODDS_API_ID, "bettable-matches/opening-odds", match_id)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI HTTP error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NosyAPI error: {e}")


@app.get("/nosy-result-details")
def nosy_result_details(
    match_id: int = Query(..., description="Nosy MatchID (ör: 151738)"),
):
    """
    Maç sonucu detayları (result API):
    Dokümanda: /bettable-result/details
    """
    try:
        return nosy_request_with_matchid_fallback(NOSY_RESULTS_API_ID, "bettable-result/details", match_id)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"NosyAPI HTTP error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NosyAPI error: {e}")


@app.get("/nosy-matches-by-date")
def nosy_matches_by_date(
    date: str = Query(..., description="YYYY-MM-DD"),
    db: Session = Depends(get_db),
):
    """
    Tarihe göre maç listesi:
    Dokümanda: /bettable-matches/date
    Bu endpoint'ten gelen MatchID'leri DB'ye upsert ederiz.
    """
    try:
        # Nosy bazı endpointlerde parametre adı "date" ya da "Date" olabiliyor.
        data = nosy_request(NOSY_ODDS_API_ID, "bettable-matches/date", params={"date": date})
    except Exception:
        # fallback dene
        data = nosy_request(NOSY_ODDS_API_ID, "bettable-matches/date", params={"Date": date})

    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        items = data["data"]
        upserted = 0
        for it in items:
            mid = it.get("MatchID") or it.get("matchID") or it.get("matchId")
            if not mid:
                continue

            # datetime parse
            dt = None
            dt_str = it.get("DateTime") or it.get("datetime") or it.get("DateTimeStr")
            if isinstance(dt_str, str) and dt_str.strip():
                # ör: "2024-02-01 23:00:00"
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        dt = datetime.strptime(dt_str.strip(), fmt)
                        break
                    except ValueError:
                        pass

            row = db.query(Match).filter(Match.match_id == int(mid)).one_or_none()
            if row is None:
                row = Match(match_id=int(mid))
                db.add(row)

            row.datetime = dt
            row.league = it.get("League")
            row.country = it.get("Country")
            row.team1 = it.get("Team1")
            row.team2 = it.get("Team2")

            # bazı listelerde odds alanları direkt geliyor
            row.home_win = _to_float(it.get("HomeWin"))
            row.draw = _to_float(it.get("Draw"))
            row.away_win = _to_float(it.get("AwayWin"))
            row.under25 = _to_float(it.get("Under25"))
            row.over25 = _to_float(it.get("Over25"))

            row.raw_json = json.dumps(it, ensure_ascii=False)
            upserted += 1

        db.commit()
        return {"ok": True, "date": date, "count": len(items), "upserted": upserted, "nosy": data}

    return data


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None
