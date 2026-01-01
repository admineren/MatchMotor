from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

import traceback
import re
import os
import secrets
import pandas as pd
import math
import logging
from typing import Optional
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, Date, Float, DateTime

logger = logging.getLogger(__name__)

# -----------------------
# DB setup
# -----------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env is missing")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def run_migrations():
    logger.info("DB migration başlatılıyor...")
    Base.metadata.create_all(bind=engine)
    logger.info("DB migration tamamlandı.")
    
Base = declarative_base()

class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True, index=True)

    match_date = Column(Date, index=True)
    league = Column(String, index=True)
    home_team = Column(String, index=True)
    away_team = Column(String, index=True)

    iy_score = Column(String)   # "1-0" gibi
    ms_score = Column(String)   # "2-1" gibi

    iy1 = Column(Float)
    iy0 = Column(Float)
    iy2 = Column(Float)

    ms1 = Column(Float)
    ms0 = Column(Float)
    ms2 = Column(Float)

    btts_yes = Column(Float)
    btts_no = Column(Float)
    under25 = Column(Float)
    over25 = Column(Float)

    created_at = Column(DateTime, default=datetime.utcnow)  # UTC naive

# -----------------------
# App + Auth
# -----------------------
security = HTTPBasic()

# Swagger'ı otomatik kapatıyoruz (biz kendimiz /docs açacağız)
app = FastAPI(
    title="MatchMotor API",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

Base.metadata.create_all(bind=engine)

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    admin_user = os.getenv("ADMIN_USER", "")
    admin_pass = os.getenv("ADMIN_PASS", "")

    correct_user = secrets.compare_digest(credentials.username, admin_user)
    correct_pass = secrets.compare_digest(credentials.password, admin_pass)

    if not (correct_user and correct_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# Excel dosyası: repo kökü/data/SadeOran.xlsx
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # apps/api/main.py -> repo root
FILE_PATH = os.path.abspath(os.path.join(BASE_DIR, "data", "SadeOran.xlsx"))
print("Using file:", FILE_PATH)

# -----------------------
# Helpers
# -----------------------
def normalize_odds(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            s = s.str.replace(",", ".", regex=False)
            s = s.replace({"": None, "nan": None, "None": None, "-": None})
            df[c] = pd.to_numeric(s, errors="coerce")
    return df

def parse_score_home_away(score):
    """'4-2' / '4 : 2' gibi skor metninden (ev, dep) tuple döndürür."""
    if score is None or (isinstance(score, float) and math.isnan(score)):
        return (None, None)
    s = str(score).strip()
    m = re.match(r"^\s*(\d+)\s*[-:]\s*(\d+)\s*$", s)
    if not m:
        return (None, None)
    return (int(m.group(1)), int(m.group(2)))

def parse_score_total_goals(x):
    """MS Skor'dan toplam gol (int) döndürür."""
    h, a = parse_score_home_away(x)
    if h is None or a is None:
        return None
    return int(h + a)

def parse_score_1x2(x):
    """Skor -> 1/0/2 (ev/beraber/deplasman)"""
    h, a = parse_score_home_away(x)
    if h is None or a is None:
        return None
    if h > a:
        return 1
    if h == a:
        return 0
    return 2

def parse_kg_result_from_score(x):
    """BTTS sonucu: 'var' / 'yok'"""
    h, a = parse_score_home_away(x)
    if h is None or a is None:
        return None
    return "var" if (h > 0 and a > 0) else "yok"

def build_iy_ms_key(iy_score, ms_score):
    """İY ve MS sonucunu birleştir: örn '1/0' gibi"""
    iy_res = parse_score_1x2(iy_score)
    ms_res = parse_score_1x2(ms_score)
    if iy_res is None or ms_res is None:
        return None
    return f"{iy_res}/{ms_res}"

def to_float(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def raise_500_with_trace(e: Exception):
    tb = traceback.format_exc()
    raise HTTPException(
        status_code=500,
        detail={"error": str(e), "traceback": tb},
    )

# -----------------------
# Endpoints
# -----------------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/debug/insert-test-match")
def insert_test_match(
    db: Session = Depends(get_db),
    user: str = Depends(authenticate),
):
    try:
        m = Match(
            match_date=date.today(),
            league="TEST LIG",
            home_team="A",
            away_team="B",
            iy_score="0-0",
            ms_score="1-0",
            iy1=2.10,
            iy0=1.90,
            iy2=3.40,
            ms1=1.80,
            ms0=3.20,
            ms2=4.50,
        )
        db.add(m)
        db.commit()
        db.refresh(m)
        return {"inserted": True, "id": m.id}
    except Exception as e:
        db.rollback()
        raise_500_with_trace(e)

@app.get("/db-health")
def db_health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"db": "ok"}
    except Exception as e:
        return {"db": "error", "detail": str(e)}

@app.post("/import/file")
def import_file(file: UploadFile = File(...), db: Session = Depends(get_db), user: str = Depends(authenticate)):
    """
    Excel dosyasını (xlsx/xls) okuyup Match tablosuna toplu şekilde yazar.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Sadece Excel (.xlsx/.xls) dosyası destekleniyor")

    tmp_path = f"/tmp/{file.filename}"
    try:
        with open(tmp_path, "wb") as f:
            f.write(file.file.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dosya kaydedilemedi: {e}")

    try:
        df = pd.read_excel(tmp_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Excel okunamadı: {e}")

    if df.empty:
        return {"inserted": 0, "message": "Dosya boş"}

    df.columns = [str(c).strip() for c in df.columns]

    required = ["Saat", "Lig", "Ev", "Deplasman", "İY Skor", "MS Skor"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Eksik sütun(lar): {missing}. Mevcut sütunlar: {list(df.columns)}",
        )

    inserted = 0
    failed_rows = 0
    batch = []
    BATCH_SIZE = 1000

    try:
        for row in df.itertuples(index=False):
            r = row._asdict() if hasattr(row, "_asdict") else dict(zip(df.columns, row))

            m = Match(
                match_date=date.today(),  # dosyada tarih yoksa bugünü yazıyoruz
                league=str(r.get("Lig", "")).strip() or None,
                home_team=str(r.get("Ev", "")).strip() or None,
                away_team=str(r.get("Deplasman", "")).strip() or None,
                iy_score=str(r.get("İY Skor", "")).strip() or None,
                ms_score=str(r.get("MS Skor", "")).strip() or None,

                ms1=to_float(r.get("MS1")),
                ms0=to_float(r.get("MS0")),
                ms2=to_float(r.get("MS2")),

                under25=to_float(r.get("2.5 Alt")),
                over25=to_float(r.get("2.5 Üst")),

                btts_yes=to_float(r.get("KG Var")),
                btts_no=to_float(r.get("KG Yok")),
            )

            batch.append(m)

            if len(batch) >= BATCH_SIZE:
                db.bulk_save_objects(batch)
                db.commit()
                inserted += len(batch)
                batch.clear()

        if batch:
            db.bulk_save_objects(batch)
            db.commit()
            inserted += len(batch)
            batch.clear()

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB yazma hatası: {e}")
    except Exception as e:
        db.rollback()
        raise_500_with_trace(e)

    return {"inserted": inserted, "failed_rows": failed_rows}

@app.get("/matches")
def list_matches(
    user: str = Depends(authenticate),
    lig: Optional[str] = None,
    ligs: Optional[str] = None,  # virgülle ayrılmış liste
    limit: int = 20,
    tg_filter: Optional[str] = None,  # "0-1", "2-3", "4-5", "6+"
    kg: Optional[str] = None,  # "var" / "yok"
    kg_var_min: Optional[float] = None,
    kg_var_max: Optional[float] = None,
    kg_yok_min: Optional[float] = None,
    kg_yok_max: Optional[float] = None,
    ms1_min: Optional[float] = None,
    ms1_max: Optional[float] = None,
    ms0_min: Optional[float] = None,
    ms0_max: Optional[float] = None,
    ms2_min: Optional[float] = None,
    ms2_max: Optional[float] = None,
    iy1_min: Optional[float] = None,
    iy1_max: Optional[float] = None,
    iy0_min: Optional[float] = None,
    iy0_max: Optional[float] = None,
    iy2_min: Optional[float] = None,
    iy2_max: Optional[float] = None,
    iy: Optional[int] = None,
    ms: Optional[int] = None,
):
    # ✅ Senin Render hatanın asıl sebebi şuydu:
    # Bu endpointte "try:" vardı ama "except/finally" yoktu.
    # Python dosyayı parse edemeyip, sonraki decorator'da "expected except or finally block" diye patlıyordu.
    try:
        if limit < 1:
            limit = 1
        if limit > 500:
            limit = 500

        df = pd.read_excel(FILE_PATH)

        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.replace("İ", "I", regex=False)
            .str.replace("ı", "i", regex=False)
        )

        df = normalize_odds(df, ["MS1", "MS0", "MS2", "IY 1", "IY 0", "IY 2", "KG Var", "KG Yok"])

        if "MS Skor" in df.columns:
            df["_tg"] = df["MS Skor"].apply(parse_score_total_goals)
            df["_kg_res"] = df["MS Skor"].apply(parse_kg_result_from_score)
        else:
            df["_tg"] = None
            df["_kg_res"] = None

        if tg_filter and "_tg" in df.columns:
            s = str(tg_filter).strip()
            if s == "0-1":
                df = df[(df["_tg"] >= 0) & (df["_tg"] <= 1)]
            elif s == "2-3":
                df = df[(df["_tg"] >= 2) & (df["_tg"] <= 3)]
            elif s == "4-5":
                df = df[(df["_tg"] >= 4) & (df["_tg"] <= 5)]
            elif s in ("6+", "6"):
                df = df[df["_tg"] >= 6]

        if "IY Skor" in df.columns and "MS Skor" in df.columns:
            df["_iy_ms"] = df.apply(lambda r: build_iy_ms_key(r["IY Skor"], r["MS Skor"]), axis=1)
        else:
            df["_iy_ms"] = None

        if iy is not None or ms is not None:
            df["_iy_res"] = df["IY Skor"].apply(parse_score_1x2) if "IY Skor" in df.columns else None
            df["_ms_res"] = df["MS Skor"].apply(parse_score_1x2) if "MS Skor" in df.columns else None
            if iy is not None:
                df = df[df["_iy_res"] == int(iy)]
            if ms is not None:
                df = df[df["_ms_res"] == int(ms)]

        if lig and "Lig" in df.columns:
            df = df[df["Lig"].astype(str) == str(lig)]
        elif ligs and "Lig" in df.columns:
            lig_list = [x.strip() for x in str(ligs).split(",") if x.strip()]
            if lig_list:
                df = df[df["Lig"].astype(str).isin(lig_list)]

        def apply_range(col: str, vmin: Optional[float], vmax: Optional[float]) -> None:
            nonlocal df
            if col not in df.columns:
                return
            if vmin is not None:
                df = df[df[col] >= vmin]
            if vmax is not None:
                df = df[df[col] <= vmax]

        apply_range("MS1", ms1_min, ms1_max)
        apply_range("MS0", ms0_min, ms0_max)
        apply_range("MS2", ms2_min, ms2_max)

        apply_range("IY 1", iy1_min, iy1_max)
        apply_range("IY 0", iy0_min, iy0_max)
        apply_range("IY 2", iy2_min, iy2_max)

        apply_range("KG Var", kg_var_min, kg_var_max)
        apply_range("KG Yok", kg_yok_min, kg_yok_max)

        if kg:
            kg_s = str(kg).strip().lower()
            if kg_s in ("var", "yok"):
                if "_kg_res" not in df.columns and "MS Skor" in df.columns:
                    df["_kg_res"] = df["MS Skor"].apply(parse_kg_result_from_score)
                df = df[df["_kg_res"] == kg_s]

        tg_series = df["_tg"].dropna() if "_tg" in df.columns else pd.Series([], dtype=float)
        gol_dist = {
            "0-1": int(((tg_series >= 0) & (tg_series <= 1)).sum()),
            "2-3": int(((tg_series >= 2) & (tg_series <= 3)).sum()),
            "4-5": int(((tg_series >= 4) & (tg_series <= 5)).sum()),
            "6+": int((tg_series >= 6).sum()),
        } if len(tg_series) else {}

        kg_dist = {
            "var": int((df["_kg_res"] == "var").sum()) if "_kg_res" in df.columns else 0,
            "yok": int((df["_kg_res"] == "yok").sum()) if "_kg_res" in df.columns else 0,
        }

        iy_ms_dist = df["_iy_ms"].dropna().value_counts().to_dict() if "_iy_ms" in df.columns else {}

        total = int(len(df))
        rows = df.head(limit).to_dict(orient="records")
        returned = int(len(rows))

        return {
            "total": total,
            "returned": returned,
            "limit": limit,
            "goal_dist": gol_dist,
            "kg_dist": kg_dist,
            "iy_ms_dist": iy_ms_dist,
            "matches": rows,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise_500_with_trace(e)

@app.get("/daily-summary")
def daily_summary(
    day: Optional[str] = None,  # "2026-01-01" formatında; boşsa bugün (TR)
    user: str = Depends(authenticate),
    db: Session = Depends(get_db),
):
    try:
        tz_tr = ZoneInfo("Europe/Istanbul")
        tz_utc = ZoneInfo("UTC")

        if day:
            try:
                d = datetime.strptime(day, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail="day format must be YYYY-MM-DD")
        else:
            d = datetime.now(tz_tr).date()

        start_tr = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz_tr)
        end_tr = start_tr + timedelta(days=1)

        start_utc = start_tr.astimezone(tz_utc).replace(tzinfo=None)
        end_utc = end_tr.astimezone(tz_utc).replace(tzinfo=None)

        total = db.query(func.count(Match.id)).scalar() or 0

        added_today = (
            db.query(func.count(Match.id))
            .filter(Match.created_at >= start_utc, Match.created_at < end_utc)
            .scalar()
            or 0
        )

        ms_ok = (
            db.query(func.count(Match.id))
            .filter(
                Match.created_at >= start_utc, Match.created_at < end_utc,
                Match.ms1.isnot(None),
                Match.ms0.isnot(None),
                Match.ms2.isnot(None),
            )
            .scalar()
            or 0
        )

        btts_ok = (
            db.query(func.count(Match.id))
            .filter(
                Match.created_at >= start_utc, Match.created_at < end_utc,
                Match.btts_yes.isnot(None),
                Match.btts_no.isnot(None),
            )
            .scalar()
            or 0
        )

        ou25_ok = (
            db.query(func.count(Match.id))
            .filter(
                Match.created_at >= start_utc, Match.created_at < end_utc,
                Match.over25.isnot(None),
                Match.under25.isnot(None),
            )
            .scalar()
            or 0
        )

        return {
            "day_tr": d.isoformat(),
            "range_tr": {"start": start_tr.isoformat(), "end": end_tr.isoformat()},
            "range_utc_used_in_db": {"start": start_utc.isoformat() + "Z", "end": end_utc.isoformat() + "Z"},
            "total_matches": total,
            "added_today": added_today,
            "markets_today": {"ms_1x2_ok": ms_ok, "btts_ok": btts_ok, "ou25_ok": ou25_ok},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise_500_with_trace(e)

@app.get("/test-excel")
def test_excel(user: str = Depends(authenticate)):
    df = pd.read_excel(FILE_PATH)
    return {"rows": int(len(df)), "columns": list(df.columns)}

# Korumalı OpenAPI json
@app.get("/openapi.json")
def openapi_json(user: str = Depends(authenticate)):
    return JSONResponse(
        get_openapi(
            title=app.title,
            version="0.1.0",
            routes=app.routes,
        )
    )

# Korumalı Swagger UI
@app.get("/docs", response_class=HTMLResponse)
def docs(user: str = Depends(authenticate)):
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="MatchMotor API - Docs",
    )
