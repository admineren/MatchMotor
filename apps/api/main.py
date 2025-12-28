from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

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
def list_matches(user: str = Depends(authenticate)):
    df = pd.read_excel(FILE_PATH)
    # test için ilk 20 satır
    return df.head(20).to_dict(orient="records")


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
