# app/core/jobs.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

from .budget import BudgetTracker
from .config import Config
from .datasource import DataSource, FixtureBundle
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
    return status in {"FT"}  # gerekirse {AET, PEN} eklenebilir


def _is_ht(status: str) -> bool:
    return status in {"HT"}


def _is_ns(status: str) -> bool:
    return status in {"NS"}


def _extract_score_from_match(match: Match) -> Optional[Score]:
    """
    Şimdilik Match içinde skor yok; ileride fixtures skor verirse Match'e eklenebilir.
    Bu fonksiyon yer tutucu: ApiSource impl. ile genişleyecek.
    """
    return None


def run_job_1500(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    """
    15:00 job:
    - fixtures al
    - kick-off'a göre sırala
    - FT: skor yaz + DONE
    - HT: sadece İY yaz
    - NS: MS odds dene -> varsa DB'ye al (match + odds)
    - MS odds yoksa: elenir (DB'ye yazılmaz)
    - budget 600'de durur
    """
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    # fixtures request
    if not budget.can_consume(1):
        return JobResult(day, "15:00", 0, 0, 0, 0, 0, budget.used)
    budget.consume(1)
    bundle: FixtureBundle = ds.get_fixtures(day)

    fixtures = _sort_by_kickoff(bundle.matches)

    processed = selected = ignored = done = 0

    for m in fixtures:
        if processed >= cfg.max_matches_per_day:
            break
        if not budget.can_consume(0):
            break

        # DONE kilidi
        if repo.is_done(m.match_id):
            continue

        # PST/CANC ignore
        if _should_ignore_status(m.status):
            # İstersen bu satırı aktif edip ignore kayıtlarını DB'de tutabilirsin
            # repo.mark_ignored(m.match_id, reason=m.status)
            ignored += 1
            continue

        processed += 1

        # FT ise kapat
        if _is_ft(m.status):
            score = _extract_score_from_match(m)
            if score is not None:
                repo.save_score(m.match_id, score)
            repo.mark_done(m.match_id)
            done += 1
            continue

        # HT ise sadece İY
        if _is_ht(m.status):
            score = _extract_score_from_match(m)
            if score is not None:
                repo.save_score(m.match_id, score)
            continue

        # NS ise MS odds kapısı
        if _is_ns(m.status):
            # odds request
            if not budget.can_consume(1):
                break
            budget.consume(1)

            odds: Optional[MsOdds] = ds.get_ms_odds(m.match_id)
            if odds is None:
                ignored += 1
                continue

            # seçildi -> DB'ye yaz
            repo.upsert_match(m)
            repo.save_ms_odds(m.match_id, odds)
            selected += 1
            continue

        # 1H/2H vb. -> dokunma
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


def run_job_2300(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    """
    23:00 job:
    - fixtures al
    - FT olanları kapat (İY+MS yaz, DONE)
    - NS/oynananları elleme (ertesi güne kalır)
    - Pro aşamada ek market YOK
    """
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    # fixtures request
    if not budget.can_consume(1):
        return JobResult(day, "23:00", 0, 0, 0, 0, 0, budget.used)
    budget.consume(1)
    bundle: FixtureBundle = ds.get_fixtures(day)

    fixtures = _sort_by_kickoff(bundle.matches)

    processed = selected = ignored = done = 0

    for m in fixtures:
        if processed >= cfg.max_matches_per_day:
            break
        if repo.is_done(m.match_id):
            continue

        if _should_ignore_status(m.status):
            ignored += 1
            continue

        processed += 1

        if _is_ft(m.status):
            score = _extract_score_from_match(m)
            if score is not None:
                repo.save_score(m.match_id, score)
            repo.mark_done(m.match_id)
            done += 1
            continue

        if _is_ht(m.status):
            score = _extract_score_from_match(m)
            if score is not None:
                repo.save_score(m.match_id, score)
            continue

        # NS/1H/2H -> dokunma
        continue

    return JobResult(
        day=day,
        job_name="23:00",
        fixtures_count=len(fixtures),
        processed_count=processed,
        selected_count=selected,
        ignored_count=ignored,
        done_count=done,
        requests_used=budget.used,
  )
