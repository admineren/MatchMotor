from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi import HTTPException
import traceback
from typing import Optional

import os
import secrets
import pandas as pd

security = HTTPBasic()

# Excel dosyası: repo kökü/data/SadeOran.xlsx
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # apps/api/main.py -> repo root
FILE_PATH = os.path.join(BASE_DIR, "data", "SadeOran.xlsx")
FILE_PATH = os.path.abspath(FILE_PATH)
print("Using file:", FILE_PATH)

def _to_float_series(s: pd.Series) -> pd.Series:
    """
    Excel'den gelen oran sütunları bazen '1,25' gibi string/metin olur.
    Hepsini güvenli şekilde float'a çevirir.
    """
    if s is None:
        return s
    # önce stringe çevir, virgülü noktaya çevir, boşları NaN yap
    s2 = (
        s.astype(str)
         .str.replace(",", ".", regex=False)
         .str.replace(" ", "", regex=False)
    )
    # 'nan', '' gibi şeyler NaN'a dönsün
    s2 = s2.replace({"nan": None, "None": None, "": None})
    return pd.to_numeric(s2, errors="coerce")

def normalize_odds(df, cols):
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            s = s.str.replace(",", ".", regex=False)
            s = s.replace({"": None, "nan": None, "None": None, "-": None})
            df[c] = pd.to_numeric(s, errors="coerce")
    return df

# Swagger'ı otomatik kapatıyoruz (biz kendimiz /docs açacağız)
app = FastAPI(
    title="MatchMotor API",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


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


@app.get("/health")
def health(user: str = Depends(authenticate)):
    return {"status": "ok"}

def parse_score_total_goals(x):
    # örnek: "2-1" / "2 - 1" / "2:1"
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None

    # ":" gibi ayırıcıları "-" yap, boşlukları temizle
    s = s.replace(":", "-").replace("–", "-")
    s = s.replace(" ", "")

    parts = s.split("-")
    if len(parts) != 2:
        return None

    try:
        a = int(parts[0])
        b = int(parts[1])
        return a + b
    except:
        return None

def parse_score_1x2(x):
    """
    "2-1" / "2 - 1" / "2:1" -> "1" (ev), "0" (beraber), "2" (deplasman)
    """
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None

    s = s.replace(":", "-").replace("–", "-")
    s = s.replace(" ", "")
    parts = s.split("-")
    if len(parts) != 2:
        return None

    try:
        a = int(parts[0])
        b = int(parts[1])
    except:
        return None

    if a > b:
        return "1"
    if a == b:
        return "0"
    return "2"

def make_iy_ms(iy_skor, ms_skor):
    """
    IY Skor ve MS Skor'dan 1/1, 1/0, 2/1 gibi kombinasyon üretir
    """
    iy = parse_score_1x2(iy_skor)
    ms = parse_score_1x2(ms_skor)

    if iy is None or ms is None:
        return None

    return f"{iy}/{ms}"

def make_iy_ms(iy_res, ms_res):
    # "1" + "0" -> "1/0"
    if iy_res is None or ms_res is None:
        return None
    return f"{iy_res}/{ms_res}"

def parse_score_1x2(x):
    """
    MS Skor veya IY Skor gibi '2-1', '0 - 0', '1:2' formatlarını
    1/0/2 sonucuna çevirir.
    1 = Ev kazanır, 0 = Beraberlik, 2 = Deplasman kazanır
    """
    if x is None or pd.isna(x):
        return None

    s = str(x).strip()
    if not s:
        return None

    s = s.replace(":", "-").replace(".", "-")
    s = s.replace(" ", "")
    parts = s.split("-")
    if len(parts) != 2:
        return None

    try:
        a = int(parts[0])
        b = int(parts[1])
    except:
        return None

    if a > b:
        return 1
    if a == b:
        return 0
    return 2

def build_iy_ms_key(iy_skor, ms_skor):
    """
    IY Skor ve MS Skor'u alıp '1/1', '1/0', '0/2' gibi anahtar üretir.
    """
    iy = parse_score_1x2(iy_skor)
    ms = parse_score_1x2(ms_skor)
    if iy is None or ms is None:
        return None
    return f"{iy}/{ms}"

@app.get("/matches")
def list_matches(
    user: str = Depends(authenticate),
    lig: Optional[str] = None,
    ligs: Optional[str] = None,   # virgülle çoklu lig: "TSL,ENG1"
    limit: int = 20,
    tg_filter: Optional[str] = None,
    kg: Optional[str] = None,
    
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
):
    try:
    # güvenlik: limit 1..500
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    df = pd.read_excel(FILE_PATH)
    
    df.columns = (
        df.columns
        .str.strip()
        .str.replace("İ", "I")
        .str.replace("ı", "i")
    )
    
    # 1) oranları sayıya çevir
    df = normalize_odds(df, ["MS1","MS0","MS2","İY 1","İY 0","İY 2","KG Var","KG Yok"])
    
    # MS Skor'dan toplam gol
    if "MS Skor" in df.columns:
        df["_tg"] = df["MS Skor"].apply(parse_score_total_goals)
    else:
        df["_tg"] = None
    
    # 1.4) Toplam gol aralığı filtresi (tg_filter)
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
   
    # IY / MS sonucu (1/1, 1/0, 0/2)
    if "IY Skor" in df.columns and "MS Skor" in df.columns:
        df["_iy_ms"] = df.apply(
            lambda r: build_iy_ms_key(r["IY Skor"], r["MS Skor"]),
            axis=1
        )
    else:
        df["_iy_ms"] = None
        # 1.5) IY / MS filtreleri (iy=1, ms=2 gibi)
    if iy is not None or ms is not None:
        if "IY Skor" in df.columns and "MS Skor" in df.columns:
            # IY filtresi
            if iy is not None:
                df["_iy_res"] = df["IY Skor"].apply(parse_score_1x2)
                df = df[df["_iy_res"] == int(iy)]
                # MS filtresi
            if ms is not None:
                df["_ms_res"] = df["MS Skor"].apply(parse_score_1x2)
                df = df[df["_ms_res"] == int(ms)]
    
    # 2) lig filtresi (lig veya ligs doluysa)
    if lig:
        df = df[df["Lig"].astype(str) == lig]
    elif ligs:
        lig_list = [x.strip() for x in ligs.split(",") if x.strip()]
        if lig_list:
            df = df[df["Lig"].astype(str).isin(lig_list)]

    # 3) oran filtreleri (dolu olanlar uygulanır)
    if ms1_min is not None:
        df = df[df["MS1"] >= ms1_min]
    if ms1_max is not None:
        df = df[df["MS1"] <= ms1_max]

    if ms0_min is not None:
        df = df[df["MS0"] >= ms0_min]
    if ms0_max is not None:
        df = df[df["MS0"] <= ms0_max]

    if ms2_min is not None:
        df = df[df["MS2"] >= ms2_min]
    if ms2_max is not None:
        df = df[df["MS2"] <= ms2_max]
    
    # İY oran filtreleri
    if iy1_min is not None:
        df = df[df["İY 1"] >= iy1_min]
    if iy1_max is not None:
        df = df[df["İY 1"] <= iy1_max]
    
    if iy0_min is not None:
        df = df[df["İY 0"] >= iy0_min]
    if iy0_max is not None:
        df = df[df["İY 0"] <= iy0_max]
    
    if iy2_min is not None:
        df = df[df["İY 2"] >= iy2_min]
    if iy2_max is not None:
        df = df[df["İY 2"] <= iy2_max]
    
    # KG Var / KG Yok oran filtreleri
    if kg_var_min is not None:
        df = df[df["KG Var"] >= kg_var_min]
    if kg_var_max is not None:
        df = df[df["KG Var"] <= kg_var_max]
    if kg_yok_min is not None:
        df = df[df["KG Yok"] >= kg_yok_min]
    if kg_yok_max is not None:
        df = df[df["KG Yok"] <= kg_yok_max]
    
    # KG Var / KG Yok (tek kutu)
    if kg:
        kg_s = str(kg).strip().lower()
        
        if kg_s == "var":
            if "KG Var" in df.columns:
                df = df[df["KG Var"].notna()]
        
        elif kg_s == "yok":
            if "KG Yok" in df.columns:
                df = df[df["KG Yok"].notna()]

    # Gol dağılımı (0-1, 2-3, 4-5, 6+)
    tg_series = df["_tg"].dropna()
    gol_dist = {
        "0-1": int(((tg_series >= 0) & (tg_series <= 1)).sum()),
        "2-3": int(((tg_series >= 2) & (tg_series <= 3)).sum()),
        "4-5": int(((tg_series >= 4) & (tg_series <= 5)).sum()),
        "6+":  int((tg_series >= 6).sum()),
    }
    # IY / MS dağılımı
    iy_ms_dist = (
        df["_iy_ms"]
        .dropna()
        .value_counts()
        .to_dict()
    )
    
    total = int(len(df))
    rows = df.head(limit).to_dict(orient="records")
    returned = int(len(rows))
    
    return {
        "total": total,
        "returned": returned,
        "limit": limit,
        "goal_dist": gol_dist,
        "iy_ms_dist": iy_ms_dist,
        "matches": rows,
    }
    
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test-excel")
def test_excel(user: str = Depends(authenticate)):
    df = pd.read_excel(FILE_PATH)
    
    return {"rows": len(df), "columns": list(df.columns)}

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
