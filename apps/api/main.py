import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import json
import urllib.parse
import urllib.request
import urllib.error

from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine


# ------------------
# LOGGING
# ------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("matchmotor")


# ------------------
# ENV
# ------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_USER = os.getenv("ADMIN_USER", "").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "").strip()

NOSY_API_KEY = os.getenv("NOSY_API_KEY", "").strip()
NOSY_BASE_URL = os.getenv("NOSY_BASE_URL", "https://www.nosyapi.com/apiv2").strip()
NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID", "").strip()
NOSY_RESULTS_API_ID = os.getenv("NOSY_RESULTS_API_ID", "").strip()

# NOSY_BASE_URL varsayilan olarak: https://www.nosyapi.com/apiv2
# Servis endpointleri icin otomatik olarak /service ekliyoruz.
NOSY_ROOT_BASE_URL = NOSY_BASE_URL.rstrip("/")
NOSY_SERVICE_BASE_URL = os.getenv(
    "NOSY_SERVICE_BASE_URL",
    f"{NOSY_ROOT_BASE_URL}/service"
).strip().rstrip("/")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")


# ------------------
# DB
# ------------------
engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ------------------
# AUTH
# ------------------
security = HTTPBasic()


def require_admin(credentials: HTTPBasicCredentials = Depends(security)):
    if not ADMIN_USER or not ADMIN_PASS:
        raise HTTPException(status_code=500, detail="ADMIN_USER/ADMIN_PASS not set")
    if credentials.username != ADMIN_USER or credentials.password != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


# ------------------
# APP
# ------------------
app = FastAPI(title="MatchMotor API", version="0.1.0")


# ------------------
# SCHEMA / INIT
# ------------------
def ensure_schema():
    """
    Basit schema: matches tablosu.
    Not: Daha once 'datetime' index hatasi almistin.
    Bu versiyonda datetime index zorunlu degil; varsa kendin ekleyebilirsin.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    id SERIAL PRIMARY KEY,
                    league TEXT,
                    home TEXT,
                    away TEXT,
                    ht_score TEXT,
                    ft_score TEXT,
                    ms1 NUMERIC,
                    ms0 NUMERIC,
                    ms2 NUMERIC,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """
            )
        )

        # Sadece lig indexi (sende bu kalacak demistin)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league);"))


@app.on_event("startup")
def on_startup():
    ensure_schema()
    logger.info("Startup complete")


# ------------------
# HEALTH
# ------------------
@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}


# ------------------
# MATCHES (DB)
# ------------------
@app.get("/matches")
def list_matches(limit: int = 50, offset: int = 0):
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, league, home, away, ht_score, ft_score, ms1, ms0, ms2, created_at
                FROM matches
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        ).mappings().all()
    return {"count": len(rows), "data": list(rows)}


@app.post("/admin/matches/clear")
def clear_matches(_: bool = Depends(require_admin)):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE matches RESTART IDENTITY;"))
    return {"ok": True}


# ------------------
# NOSY HELPERS
# ------------------
def _nosy_build_url(path: str, *, use_service: bool) -> str:
    """
    NosyAPI iki farkli base kullaniyor:
      - Root:   https://www.nosyapi.com/apiv2            (or: /nosy-service/check)
      - Service:https://www.nosyapi.com/apiv2/service    (or: /bettable-matches/..., /bettable-result/...)
    """
    base = NOSY_SERVICE_BASE_URL if use_service else NOSY_ROOT_BASE_URL
    p = path.strip()
    if not p.startswith("/"):
        p = "/" + p
    return f"{base}{p}"


def _http_get_json(url: str, *, headers: Dict[str, str], timeout: int) -> Tuple[int, Dict[str, Any]]:
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            raw = resp.read().decode("utf-8", errors="replace")
            return status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        status = e.code
        raw = e.read().decode("utf-8", errors="replace") if e.fp else ""
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {"error": raw or str(e)}
        return status, data


def nosy_get(
    path: str,
    *,
    api_id: str = "",
    use_service: bool = True,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
) -> Dict[str, Any]:
    """
    NosyAPI request helper.

    Notlar:
    - Bazi endpointler apiKey + apiID query param bekliyor.
    - Bazi orneklerde Bearer ile de calisabiliyor. Biz 401 olursa fallback yapiyoruz.
    """
    if not NOSY_API_KEY:
        raise RuntimeError("NOSY_API_KEY is not set")

    base_url = _nosy_build_url(path, use_service=use_service)

    q = dict(params or {})
    q.setdefault("apiKey", NOSY_API_KEY)
    if api_id:
        q.setdefault("apiID", api_id)

    url = f"{base_url}?{urllib.parse.urlencode(q)}" if q else base_url

    # 1) Once query-param ile dene
    status, data = _http_get_json(url, headers={"accept": "application/json"}, timeout=timeout)

    # 2) 401 ise Bearer dene
    if status == 401:
        status, data = _http_get_json(
            url,
            headers={"accept": "application/json", "Authorization": f"Bearer {NOSY_API_KEY}"},
            timeout=timeout,
        )

    # 3) Hala 401 ise X-API-Key
    if status == 401:
        status, data = _http_get_json(
            url,
            headers={"accept": "application/json", "X-API-Key": NOSY_API_KEY},
            timeout=timeout,
        )

    if status >= 400:
        raise HTTPException(status_code=502, detail=f"NosyAPI request failed: HTTP {status} - {data}")

    return data


# ------------------
# NOSY ENDPOINTS
# ------------------
@app.get("/nosy-check")
def nosy_check():
    """
    Nosy servis durumu / API key kontrolu.
    Dokumandaki endpoint root altinda:
      /nosy-service/check
    """
    api_id = NOSY_RESULTS_API_ID or NOSY_ODDS_API_ID
    return nosy_get("/nosy-service/check", api_id=api_id, use_service=False)


@app.get("/nosy-opening-odds")
def nosy_opening_odds(match_id: Optional[int] = None):
    """
    Acilis oranlari.
    Endpoint (service altinda):
      /bettable-matches/opening-odds

    "Match ID not found" alirsan => match_id gondermen lazim.
    """
    params: Dict[str, Any] = {}
    if match_id is not None:
        params["matchID"] = match_id

    return nosy_get(
        "/bettable-matches/opening-odds",
        api_id=NOSY_ODDS_API_ID,
        use_service=True,
        params=params or None,
    )


@app.get("/nosy-result")
def nosy_result(match_id: Optional[int] = None):
    """
    Mac sonucu endpointi (sende 1881149 RESULTS API ID).
    Endpoint ismi dokumana gore degisebilir; burada en temel "bettable-result"i koyduk.
    """
    params: Dict[str, Any] = {}
    if match_id is not None:
        params["matchID"] = match_id

    return nosy_get(
        "/bettable-result",
        api_id=NOSY_RESULTS_API_ID,
        use_service=True,
        params=params or None,
    )
