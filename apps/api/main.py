from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
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


@app.get("/matches")
def list_matches(
    user: str = Depends(authenticate),
    lig: Optional[str] = None,
    ligs: Optional[str] = None,
    limit: int = 20,

    ms1_min: Optional[float] = None,
    ms1_max: Optional[float] = None,
    ms0_min: Optional[float] = None,
    ms0_max: Optional[float] = None,
    ms2_min: Optional[float] = None,
    ms2_max: Optional[float] = None,
):
    # Güvenlik: limit sınırı
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    df = pd.read_excel(FILE_PATH)

    # Lig filtresi (opsiyonel)
    if lig:
        df = df[df["Lig"].astype(str) == lig]

    # Çoklu lig filtresi (opsiyonel) -> ligs=CHN2,ENG1
    if ligs:
        lig_list = [x.strip() for x in ligs.split(",") if x.strip()]
        if lig_list:
            df = df[df["Lig"].astype(str).isin(lig_list)]

    # Virgüllü oranları sayıya çevir (MS1/MS0/MS2)
    def to_float_series(s):
        s = s.astype(str).str.strip()
        s = s.str.replace("\u00a0", "", regex=False)  # bazen gizli boşluk
        s = s.str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce")

    for c in ["MS1", "MS0", "MS2"]:
        if c in df.columns:
            df[c] = to_float_series(df[c])

    # Oran aralık filtreleri (manuel)
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

    return df.head(limit).to_dict(orient="records")

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
