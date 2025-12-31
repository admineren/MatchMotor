# dev_run.py
from datetime import date

from app.core.config import Config
from app.core.jobs import run_job_1500, run_job_2300
from app.core.mock_source import MockSource
from app.core.repository import NoOpRepository


def print_result(r):
    print("=" * 60)
    print(f"[DEV PANEL] Date: {r.day} | Job: {r.job_name}")
    print(f"Fixtures:        {r.fixtures_count}")
    print(f"Processed:       {r.processed_count}")
    print(f"Selected (MS):   {r.selected_count}")
    print(f"Ignored:         {r.ignored_count}")
    print(f"FT Closed (DONE):{r.done_count}")
    print(f"Requests Used:   {r.requests_used} / 600 (hard 650)")
    print("=" * 60)


if __name__ == "__main__":
    cfg = Config()
    ds = MockSource(fixtures_count=420, ms_odds_ratio=0.6, seed=42)
    repo = NoOpRepository()

    day = date.today()  # istersen sabit bir gün ver: date(2026, 1, 1)

    r1 = run_job_1500(cfg, ds, repo, day)
    print_result(r1)

    r2 = run_job_2300(cfg, ds, repo, day)
    print_result(r2)
