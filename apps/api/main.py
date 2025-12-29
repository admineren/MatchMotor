from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi import HTTPException
import traceback
import re
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


def parse_score_home_away(score):
    """'4 - 2' gibi skor metninden (ev, dep) tuple döndürür."""
    if score is None:
        return (None, None)
    s = str(score).strip()
    # bazı dosyalarda '4-2' ya da '4 : 2' gibi gelebilir
    m = re.match(r"^\s*(\d+)\s*[-:]\s*(\d+)\s*$", s)
    if not m:
        return (None, None)
    try:
        return (int(m.group(1)), int(m.group(2)))
    except Exception:
        return (None, None)

def parse_kg_result_from_score(score):
    """KG sonucu: her iki takım gol attıysa 'var' yoksa 'yok'."""
    h, a = parse_score_home_away(score)
    if h is None or a is None:
        return None
    return "var" if (h > 0 and a > 0) else "yok"


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
    ligs: Optional[str] = None,  # virgülle ayrılmış liste
    limit: int = 20,
    tg_filter: Optional[str] = None,  # "0-1", "2-3", "4-5", "6+"
    kg: Optional[str] = None,  # "var" / "yok"

    # tek-kutu KG oranları (min/max)
    kg_var_min: Optional[float] = None,
    kg_var_max: Optional[float] = None,
    kg_yok_min: Optional[float] = None,
    kg_yok_max: Optional[float] = None,

    # MS oran aralıkları
    ms1_min: Optional[float] = None,
    ms1_max: Optional[float] = None,
    ms0_min: Optional[float] = None,
    ms0_max: Optional[float] = None,
    ms2_min: Optional[float] = None,
    ms2_max: Optional[float] = None,

    # İY oran aralıkları
    iy1_min: Optional[float] = None,
    iy1_max: Optional[float] = None,
    iy0_min: Optional[float] = None,
    iy0_max: Optional[float] = None,
    iy2_min: Optional[float] = None,
    iy2_max: Optional[float] = None,

    # skor filtresi (İY ve/veya MS sonucu: 1/0/2)
    iy: Optional[int] = None,
    ms: Optional[int] = None,
):
    try:
        # güvenlik: limit 1..500
        if limit < 1:
            limit = 1
        if limit > 500:
            limit = 500

        df = pd.read_excel(FILE_PATH)

        # kolon adlarını normalize et (boşluk, Türkçe I/İ karışıklığı vs.)
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.replace("İ", "I", regex=False)
            .str.replace("ı", "i", regex=False)
        )

        # 1) oranları sayıya çevir
        df = normalize_odds(df, ["MS1", "MS0", "MS2", "IY 1", "IY 0", "IY 2", "KG Var", "KG Yok"])

        # MS Skor'dan toplam gol
        if "MS Skor" in df.columns:
            df["_tg"] = df["MS Skor"].apply(parse_score_total_goals)
            # MS Skor'dan KG sonucu (var/yok)
            df["_kg_res"] = df["MS Skor"].apply(parse_kg_result_from_score)
        else:
            df["_tg"] = None
            df["_kg_res"] = None

        # 1.4) Toplam gol aralığı filtresi
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

        # İY / MS sonucu (1/1, 1/0, 0/2...)
        if "İY Skor" in df.columns and "MS Skor" in df.columns:
                df["_iy_ms"] = df.apply(
                    lambda r: build_iy_ms_key(r["İY Skor"], r["MS Skor"]),
                    axis=1
                )
            else:
                df["_iy_ms"] = None
        
        # 1.5) İY / MS skor filtresi (iy ve/veya ms girilirse)
        if iy is not None or ms is not None:
            if "IY Skor" in df.columns:
                df["_iy_res"] = df["IY Skor"].apply(parse_score_1x2)
            else:
                df["_iy_res"] = None

            if "MS Skor" in df.columns:
                df["_ms_res"] = df["MS Skor"].apply(parse_score_1x2)
            else:
                df["_ms_res"] = None

            if iy is not None:
                df = df[df["_iy_res"] == int(iy)]
            if ms is not None:
                df = df[df["_ms_res"] == int(ms)]

        # 2) lig filtresi (lig veya ligs)
        if lig and "Lig" in df.columns:
            df = df[df["Lig"].astype(str) == str(lig)]
        elif ligs and "Lig" in df.columns:
            lig_list = [x.strip() for x in str(ligs).split(",") if x.strip()]
            if lig_list:
                df = df[df["Lig"].astype(str).isin(lig_list)]

        # 3) oran filtreleri (dolu olanları uygula)
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

        # KG Var / KG Yok (tek kutu) -> sonuç (var/yok)
        if kg:
            kg_s = str(kg).strip().lower()
            if kg_s in ("var", "yok"):
                if "_kg_res" not in df.columns:
            # güvenlik
            if "MS Skor" in df.columns:
                df["_kg_res"] = df["MS Skor"].apply(score_to_kg_result)
            else:
                df["_kg_res"] = None
                
            df = df[df["_kg_res"] == kg_s]

        # Gol dağılımı (0-1, 2-3, 4-5, 6+)
        if "_tg" in df.columns:
            tg_series = df["_tg"].dropna()
            gol_dist = {
                "0-1": int(((tg_series >= 0) & (tg_series <= 1)).sum()),
                "2-3": int(((tg_series >= 2) & (tg_series <= 3)).sum()),
                "4-5": int(((tg_series >= 4) & (tg_series <= 5)).sum()),
                "6+": int((tg_series >= 6).sum()),
            }
        else:
            gol_dist = {}
        
        # KG Var / KG Yok dağılımı
        kg_dist = {}
        
        if "KG Var" in df.columns:
            kg_dist["var"] = int(df["KG Var"].notna().sum())
        else:
            kg_dist["var"] = 0
        
        if "KG Yok" in df.columns:
            kg_dist["yok"] = int(df["KG Yok"].notna().sum())
        else:
            kg_dist["yok"] = 0

        # İY / MS dağılımı
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
            "kg_dist": kg_dist,
            "iy_ms_dist": iy_ms_dist,
            "matches": rows,
        }
    except Exception:
        # Render loglarında net görmek için stacktrace bas
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Server error while processing Excel")
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
