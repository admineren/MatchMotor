from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

from .budget import BudgetTracker
from .config import Config
from .datasource import DataSource
from .models import Match, MsOdds, Score
from .repository import Repository


@dataclass
class JobResult:
    day: date
    job_name: str
    fixtures_count: int
    processed_count: int
    selected_count: int
    ignored_count: int
    done_count: int
    requests_used: int


def _sort_by_kickoff(matches: List[Match]) -> List[Match]:
    return sorted(matches, key=lambda m: m.kickoff_utc)


def _should_ignore_status(status: str) -> bool:
    return status in {"PST", "CANC"}


def _is_ft(status: str) -> bool:
    return status == "FT"


def _is_ns(status: str) -> bool:
    return status == "NS"


# -------------------------------------------------
# 15:00 JOB – SEÇ + İŞLE
# -------------------------------------------------
def run_job_1500(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    fixtures = _sort_by_kickoff(repo.list_raw_fixtures(day))

    processed = selected = ignored = done = 0

    for m in fixtures:
        if processed >= cfg.max_matches_per_day:
            break

        if repo.is_done(m.match_id):
            continue

        if _should_ignore_status(m.status):
            repo.mark_ignored(m.match_id, m.status)
            ignored += 1
            continue

        processed += 1

        # FT → skor + odds + DONE
        if _is_ft(m.status):
            if not budget.can_consume(1):
                break
            budget.consume(1)

            odds = ds.get_ms_odds(m.match_id)
            if odds is not None:
                repo.upsert_match(m)
                repo.save_ms_odds(m.match_id, odds)

            score = ds.get_score(m.match_id)
            if score is not None:
                repo.save_score(m.match_id, score)

            repo.mark_done(m.match_id)
            done += 1
            selected += 1
            continue

        # NS → MS odds varsa seç
        if _is_ns(m.status):
            if not budget.can_consume(1):
                break
            budget.consume(1)

            odds = ds.get_ms_odds(m.match_id)
            if odds is None:
                ignored += 1
                continue

            repo.upsert_match(m)
            repo.save_ms_odds(m.match_id, odds)
            selected += 1
            continue

    return JobResult(
        day=day,
        job_name="15:00",
        fixtures_count=len(fixtures),
        processed_count=processed,
        selected_count=selected,
        ignored_count=ignored,
        done_count=done,
        requests_used=budget.used,
    )


# -------------------------------------------------
# 23:00 JOB – EKSİK TAMAMLAMA
# -------------------------------------------------
def run_job_2300(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    matches = repo.list_selected_matches(day)

    processed = ignored = done = 0

    for m in matches:
        if repo.is_done(m.match_id):
            continue

        processed += 1

        # FT → skor tamamla + DONE
        if _is_ft(m.status):
            if not repo.has_score(m.match_id):
                if budget.can_consume(1):
                    budget.consume(1)
                    score = ds.get_score(m.match_id)
                    if score is not None:
                        repo.save_score(m.match_id, score)

            repo.mark_done(m.match_id)
            done += 1
            continue

        # Odds eksikse tamamla
        if not repo.has_ms_odds(m.match_id):
            if budget.can_consume(1):
                budget.consume(1)
                odds = ds.get_ms_odds(m.match_id)
                if odds is not None:
                    repo.save_ms_odds(m.match_id, odds)

    return JobResult(
        day=day,
        job_name="23:00",
        fixtures_count=len(matches),
        processed_count=processed,
        selected_count=0,
        ignored_count=ignored,
        done_count=done,
        requests_used=budget.used,
    )
