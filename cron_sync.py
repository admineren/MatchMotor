# cron_sync.py
import os
import datetime as dt
import requests

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")

NOSY_API_KEY = os.getenv("NOSY_API_KEY")
NOSY_ODDS_API_ID = os.getenv("NOSY_ODDS_API_ID")  # maç/odds tarafı için kullandığın apiID
NOSY_SERVICE_BASE_URL = os.getenv("NOSY_SERVICE_BASE_URL")  # ör: https://www.nosyapi.com/apiv2/service
NOSY_ROOT_BASE_URL = os.getenv("NOSY_ROOT_BASE_URL")        # ör: https://www.nosyapi.com/apiv2

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")
if not NOSY_API_KEY:
    raise RuntimeError("NOSY_API_KEY missing")
if not NOSY_ODDS_API_ID:
    raise RuntimeError("NOSY_ODDS_API_ID missing")
if not NOSY_SERVICE_BASE_URL:
    raise RuntimeError("NOSY_SERVICE_BASE_URL missing")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def join_url(base: str, endpoint: str) -> str:
    base = base.rstrip("/")
    endpoint = endpoint.lstrip("/")
    return f"{base}/{endpoint}"

def nosy_get(endpoint: str, params: dict) -> dict:
    url = join_url(NOSY_SERVICE_BASE_URL, endpoint)
    q = dict(params)
    q["apiKey"] = NOSY_API_KEY
    q["apiID"] = NOSY_ODDS_API_ID
    r = requests.get(url, params=q, timeout=30)
    # Nosy bazen 200 içinde failure döndürebiliyor; json'u alıp biz bakacağız
    try:
        return r.json()
    except Exception:
        return {"status": "failure", "raw": r.text, "http": r.status_code, "url": str(r.url)}

def upsert_nosy_matches(items: list, fetched_at: str):
    """
    nosy_matches tablosuna match listesi basar (upsert).
    Kolonlar senin mevcut şemaya göre: nosy_match_id, match_datetime, date, time, league, country,
    team1, team2, home_win, draw, away_win, under25, over25, betcount, fetched_at
    """
    sql = text("""
        INSERT INTO nosy_matches (
            nosy_match_id, match_datetime, date, time, league, country,
            team1, team2,
            home_win, draw, away_win,
            under25, over25,
            betcount,
            fetched_at
        )
        VALUES (
            :nosy_match_id, :match_datetime, :date, :time, :league, :country,
            :team1, :team2,
            :home_win, :draw, :away_win,
            :under25, :over25,
            :betcount,
            :fetched_at
        )
        ON CONFLICT (nosy_match_id)
        DO UPDATE SET
            match_datetime = EXCLUDED.match_datetime,
            date = EXCLUDED.date,
            time = EXCLUDED.time,
            league = EXCLUDED.league,
            country = EXCLUDED.country,
            team1 = EXCLUDED.team1,
            team2 = EXCLUDED.team2,
            home_win = EXCLUDED.home_win,
            draw = EXCLUDED.draw,
            away_win = EXCLUDED.away_win,
            under25 = EXCLUDED.under25,
            over25 = EXCLUDED.over25,
            betcount = EXCLUDED.betcount,
            fetched_at = EXCLUDED.fetched_at
    """)

    rows = []
    for m in items:
        # Nosy response alanlarına göre (sende gördüğümüz örnekler)
        nosy_match_id = m.get("MatchID")
        if not nosy_match_id:
            continue

        date_s = m.get("Date") or ""
        time_s = m.get("Time") or ""
        dt_s = m.get("DateTime") or (f"{date_s} {time_s}".strip())

        rows.append({
            "nosy_match_id": int(nosy_match_id),
            "match_datetime": dt_s,       # TEXT/TIMESTAMP her iki durumda da genelde kabul eder
            "date": date_s,
            "time": time_s,
            "league": m.get("League"),
            "country": m.get("Country"),
            "team1": m.get("Team1"),
            "team2": m.get("Team2"),
            "home_win": m.get("HomeWin"),
            "draw": m.get("Draw"),
            "away_win": m.get("AwayWin"),
            "under25": m.get("Under25"),
            "over25": m.get("Over25"),
            "betcount": m.get("BetCount"),
            "fetched_at": fetched_at,
        })

    if not rows:
        return 0

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)

def main():
    fetched_at = dt.datetime.utcnow().isoformat()

    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)

    total = 0
    for d in (today, tomorrow):
        payload = nosy_get("bettable-matches/date", params={"date": d.isoformat()})
        data = payload.get("data") or []
        if isinstance(data, list) and data:
            total += upsert_nosy_matches(data, fetched_at=fetched_at)
        # küçük bir log (Render cron logs)
        print(f"[{fetched_at}] date={d.isoformat()} status={payload.get('status')} rowCount={payload.get('rowCount')} upserted={len(data) if isinstance(data,list) else 0}")

    print(f"[{fetched_at}] DONE total_upsert_attempt={total}")

if __name__ == "__main__":
    main()
