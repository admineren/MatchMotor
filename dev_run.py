from datetime import date

from app.core.config import Config
from app.core.jobs import run_job_0100, run_job_1500, run_job_2300
from app.core.mock_source import MockSource
from app.core.in_memory_repository import InMemoryRepository


def main():
    cfg = Config(
        max_daily_requests=650,
        hard_api_limit=650,
        max_matches_per_day=600,
        job_time_1="15:00",
        job_time_2="23:00",
    )

    ds = MockSource(seed=42, fixtures_count=420, ms_odds_ratio=0.85, ft_ratio=0.20)
    repo = InMemoryRepository()

    day = date.today()

    r1 = run_job_0100(cfg, ds, repo, day)
    r2 = run_job_1500(cfg, ds, repo, day)
    r3 = run_job_2300(cfg, ds, repo, day)

    print("\n=== DEV RUN RESULTS ===")
    print(r1)
    print(r2)
    print(r3)

    print("\n=== PANEL ===")
    print("today_added_count:", repo.today_added_count(day))
    print("total_matches_count:", repo.total_matches_count())


if __name__ == "__main__":
    main()
