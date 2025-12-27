from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import os
import secrets
FILE_PATH = "data/SadeOran.xlsx"
print("Using file:", FILE_PATH)
app = FastAPI(title="MatchMotor API")

security = HTTPBasic()

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    admin_user = os.getenv("ADMIN_USER", "admin")
    admin_pass = os.getenv("ADMIN_PASSWORD", "")

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
    return []
