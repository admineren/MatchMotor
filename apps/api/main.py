from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

import os
import re
import math
import secrets
import logging
import traceback
from typing import Optional
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from sqlalchemy import create_engine, text, func, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base

# ------------------------
# Logging
# ------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ------------------------
# App + Auth
# ------------------------
security = HTTPBasic()

app = FastAPI(
    title="MatchMotor API",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

def authenticate(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    admin_user = os.getenv("ADMIN_USER", "")
    admin_pass = os.getenv("ADMIN_PASS", "")
    if not admin_user or not admin_pass:
        # Prod'da env yoksa yanlışlıkla açık kalmasın
        raise HTTPException(status_code=500, detail="ADMIN_USER / ADMIN_PASS env missing")

    is_user_ok = secrets.compare_digest(credentials.username, admin_user)
    is_pass_ok = secrets.compare_digest(credentials.password, admin_pass)
    if not (is_user_ok and is_pass_ok):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

# ------------------------
# DB setup
# ------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

Base = declarative_base()

# NOTE:
# DB'nizde zaten matches tablosu var. Bu model "minimum" kolonlarla çalışır.
# Eksik kolonlar varsa sorun etmez; /matches endpoint'i SQL ile okuyor.
class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True, index=True)

    # Temel
    match_date = Column(Date, index=True, nullable=True)
    time = Column(String, nullable=True)
    league = Column(String, index=True, nullable=True)
    home_team = Column(String, index=True, nullable=True)
    away_team = Column(String, index=True, nullable=True)

    # Skorlar (string tutuluyor olabilir: "1 - 0")
    iy_score = Column(String, nullable=True)
    ms_score = Column(String, nullable=True)

    # Odds (örnek)
    ms1 = Column(Float, nullable=True)
    ms0 = Column(Float, nullable=True)
    ms2 = Column(Float, nullable=True)

    iy1 = Column(Float, nullable=True)
    iy0 = Column(Float, nullable=True)
    iy2 = Column(Float, nullable=True)

    kg_var = Column(Float, nullable=True)  # BTTS Yes
    kg_yok = Column(Float, nullable=True)  # BTTS No

    ou15_alt = Column(Float, nullable=True)
    ou15_ust = Column(Float, nullable=True)
    ou25_alt = Column(Float, nullable=True)
    ou25_ust = Column(Float, nullable=True)
    ou35_alt = Column(Float, nullable=True)
    ou35_ust = Column(Float, nullable=True)

    # Goal range örnek kolonlar (0-1, 2-3, 4-5, +6)
    gr_01 = Column(Float, nullable=True)
    gr_23 = Column(Float, nullable=True)
    gr_45 = Column(Float, nullable=True)
    gr_6p = Column(Float, nullable=True)

# ------------------------
# One-time "migration" (create missing tables only)
# ------------------------
# Render'da free plan'da disk yoksa bile Postgres var; create_all sadece eksik tablo yaratır.
@app.on_event("startup")
def run_migrations():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("DB migration: create_all OK")
    except Exception:
        logger.exception("DB migration failed")

# ------------------------
# Helpers
# ------------------------
def tr_now() -> datetime:
    return datetime.now(ZoneInfo("Europe/Istanbul"))

def _fmt_tr_float(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        # 2 basamak, TR virgül
        return f"{float(x):.2f}".replace(".", ",")
    except Exception:
        return x

def _safe_day_parse(day: str) -> date:
    try:
        return datetime.strptime(day, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="day format must be YYYY-MM-DD")

def _columns_in_matches() -> set[str]:
    insp = inspect(engine)
    cols = {c["name"] for c in insp.get_columns("matches")}
    return cols

# ------------------------
# Routes
# ------------------------
@app.get("/health")
def health():
    return {"ok": True, "time_tr": tr_now().isoformat()}

@app.get("/matches")
def list_matches(
    limit: int = 20,
    user: str = Depends(authenticate),
):
    """
    DB'deki matches tablosundan ilk `limit` satırı döndürür.
    Ayrıca dağılımlar (goal_dist, kg_dist, iy_ms_dist) verir.
    """
    try:
        limit = max(1, min(int(limit), 200))
        df = pd.read_sql(text("SELECT * FROM matches ORDER BY id DESC LIMIT :lim"), engine, params={"lim": limit})

        total = int(pd.read_sql(text("SELECT COUNT(*) AS c FROM matches"), engine)["c"].iloc[0])

        # Dağılımlar için helper kolonlar (varsa)
        def _dist(col):
            if col not in df.columns:
                return {}
            return df[col].dropna().value_counts().to_dict()

        # Goal range dist: gr_col veya direkt "gol_aralik" gibi kolon varsa
        # Burada sizde _gol_range veya benzeri olabilir. Yoksa boş döner.
        goal_dist = _dist("_gol_range") or _dist("gol_range") or {}

        kg_dist = _dist("_kg_res") or _dist("kg_res") or {}

        iy_ms_dist = _dist("_iy_ms") or _dist("iy_ms") or {}

        # Satırları "panel" formatına yaklaştır (mevcut kolon adlarına göre)
        rows = []
        for _, r in df.iterrows():
            row = {}
            # En yaygın kolon isimleri (sizinkiler farklıysa, burayı büyütürüz)
            # Saat/Lig/Ev/Deplasman
            row["Saat"] = r.get("time") or r.get("Saat")
            row["Lig"] = r.get("league") or r.get("Lig")
            row["Ev"] = r.get("home_team") or r.get("Ev")
            row["Deplasman"] = r.get("away_team") or r.get("Deplasman")

            row["İY Skor"] = r.get("iy_score") or r.get("İY Skor") or r.get("IY Skor")
            row["MS Skor"] = r.get("ms_score") or r.get("MS Skor")

            # 1X2
            row["MS1"] = r.get("ms1") if "ms1" in df.columns else r.get("MS1")
            row["MS0"] = r.get("ms0") if "ms0" in df.columns else r.get("MS0")
            row["MS2"] = r.get("ms2") if "ms2" in df.columns else r.get("MS2")

            # İY 1X2
            row["İY 1"] = r.get("iy1") if "iy1" in df.columns else r.get("İY 1")
            row["İY 0"] = r.get("iy0") if "iy0" in df.columns else r.get("İY 0")
            row["İY 2"] = r.get("iy2") if "iy2" in df.columns else r.get("İY 2")

            # KG (BTTS)
            row["KG Var"] = r.get("kg_var") if "kg_var" in df.columns else r.get("KG Var")
            row["KG Yok"] = r.get("kg_yok") if "kg_yok" in df.columns else r.get("KG Yok")

            # OU Türkçe isimlendirme (Over/Under yerine Üst/Alt)
            # Siz DB'de "2.5 Over" vs tutmak istemiyorsunuz: burada sadece response label değişiyor.
            row["1.5 Alt"] = r.get("ou15_alt") if "ou15_alt" in df.columns else r.get("1.5 Alt")
            row["1.5 Üst"] = r.get("ou15_ust") if "ou15_ust" in df.columns else r.get("1.5 Üst")
            row["2.5 Alt"] = r.get("ou25_alt") if "ou25_alt" in df.columns else r.get("2.5 Alt")
            row["2.5 Üst"] = r.get("ou25_ust") if "ou25_ust" in df.columns else r.get("2.5 Üst")
            row["3.5 Alt"] = r.get("ou35_alt") if "ou35_alt" in df.columns else r.get("3.5 Alt")
            row["3.5 Üst"] = r.get("ou35_ust") if "ou35_ust" in df.columns else r.get("3.5 Üst")

            # Goal ranges
            row["0-1"] = r.get("gr_01") if "gr_01" in df.columns else r.get("0-1")
            row["2-3"] = r.get("gr_23") if "gr_23" in df.columns else r.get("2-3")
            row["4-5"] = r.get("gr_45") if "gr_45" in df.columns else r.get("4-5")
            row["+6"] = r.get("gr_6p") if "gr_6p" in df.columns else r.get("+6")

            # TR format
            for k, v in list(row.items()):
                if isinstance(v, (int, float)):
                    row[k] = _fmt_tr_float(v) if ("Alt" in k or "Üst" in k or k in {"MS1","MS0","MS2","İY 1","İY 0","İY 2","KG Var","KG Yok","0-1","2-3","4-5","+6"}) else v
            # debug fields if exist
            if "_tg" in df.columns: row["_tg"] = r.get("_tg")
            if "_kg_res" in df.columns: row["_kg_res"] = r.get("_kg_res")
            if "_iy_ms" in df.columns: row["_iy_ms"] = r.get("_iy_ms")

            rows.append(row)

        return {
            "total": total,
            "returned": len(rows),
            "limit": limit,
            "goal_dist": goal_dist,
            "kg_dist": kg_dist,
            "iy_ms_dist": iy_ms_dist,
            "matches": rows,
        }
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})

@app.get("/daily-summary")
def daily_summary(
    day: Optional[str] = None,  # "2026-01-01" ; boşsa bugün (TR)
    user: str = Depends(authenticate),
    db: Session = Depends(get_db),
):
    """
    Seçilen gün için (TR takvimi) kaç maç var, hangi marketler dolu gibi özet döner.
    500 almanızın sebebi genelde: DB'de olmayan kolonlara (ou25_over gibi) SQL ile bakılması.
    Burada kolonları dinamik kontrol ediyoruz.
    """
    try:
        d = _safe_day_parse(day) if day else tr_now().date()

        # Bugün için toplam
        total_today = db.query(func.count(Match.id)).filter(Match.match_date == d).scalar() or 0

        # "Added today" için elinizde created_at yoksa aynı şey olur.
        # İleride created_at eklersen bunu güncelleriz.
        added_today = int(total_today)

        cols = _columns_in_matches()

        def count_not_null(colname: str) -> int:
            if colname not in cols:
                return 0
            q = db.execute(
                text(f"SELECT COUNT(*) FROM matches WHERE match_date = :d AND {colname} IS NOT NULL"),
                {"d": d},
            )
            return int(q.scalar() or 0)

        markets_today = {
            "ms_1x2_ok": count_not_null("ms1") if "ms1" in cols else count_not_null("MS1"),
            "btts_ok": (count_not_null("kg_var") if "kg_var" in cols else count_not_null("KG Var")),
            "ou25_ok": (count_not_null("ou25_ust") if "ou25_ust" in cols else count_not_null("2.5 Üst")),
        }

        return {
            "day_tr": d.isoformat(),
            "total_matches_today": int(total_today),
            "added_today": int(added_today),
            "markets_today": markets_today,
            "note": "markets_today sayımları DB'de kolon varsa yapılır; yoksa 0 döner.",
        }
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})

# OpenAPI JSON (korumalı)
@app.get("/openapi.json")
def openapi_json(user: str = Depends(authenticate)):
    return JSONResponse(
        get_openapi(
            title=app.title,
            version="0.1.0",
            routes=app.routes,
        )
    )

# Swagger UI (korumalı)
@app.get("/docs", response_class=HTMLResponse)
def docs(user: str = Depends(authenticate)):
    return get_swagger_ui_html(openapi_url="/openapi.json", title="MatchMotor API - Docs")
