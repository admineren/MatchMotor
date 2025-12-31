# app/core/jobs.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List

from .budget import BudgetTracker
from .config import Config
from .datasource import DataSource, FixtureBundle
from .models import Match
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


# -----------------------------
# Helpers
# -----------------------------
def _sort_by_kickoff(matches: List[Match]) -> List[Match]:
    return sorted(matches, key=lambda m: m.kickoff_utc)


def _should_ignore_status(status: str) -> bool:
    return status in {"PST", "CANC"}


def _is_ft(status: str) -> bool:
    return status == "FT"


def _is_ns(status: str) -> bool:
    return status == "NS"


def _refresh_status_map(ds: DataSource, day: date) -> Dict[int, str]:
    """
    Güncel status map'i almak için tek sefer fixtures çağrısı.
    match_id -> status
    """
    bundle: FixtureBundle = ds.get_fixtures(day)
    return {m.match_id: m.status for m in bundle.matches}


def _apply_status(matches: List[Match], status_map: Dict[int, str]) -> List[Match]:
    """
    Raw havuzdaki Match'leri, API'den gelen güncel status ile "kopya" olarak günceller.
    Dataclass mutable ise direkt set yapılabilir; burada güvenli olması için yeni Match üretmiyoruz,
    sadece varsa status'u overwrite ediyoruz.
    """
    for m in matches:
        if m.match_id in status_map:
            m.status = status_map[m.match_id]
    return matches


# -------------------------------------------------
# 01:00 JOB – RAW HAVUZU DOLDUR
# -------------------------------------------------
def run_job_0100(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    # 1 request: fixtures
    if not budget.can_consume(1):
        return JobResult(
            day=day,
            job_name="01:00",
            fixtures_count=0,
            processed_count=0,
            selected_count=0,
            ignored_count=0,
            done_count=0,
            requests_used=budget.used,
        )

    budget.consume(1)
    bundle: FixtureBundle = ds.get_fixtures(day)
    fixtures = _sort_by_kickoff(bundle.matches)

    processed = ignored = 0

    for m in fixtures:
        processed += 1
        # Raw havuza kaydet (ignore bile olsa raw dursun)
        repo.save_raw_fixture(m)

        # İstersen raw’da da ignore işaretleyebilirsin (opsiyonel)
        if _should_ignore_status(m.status):
            repo.mark_ignored(m.match_id, m.status)
            ignored += 1

    return JobResult(
        day=day,
        job_name="01:00",
        fixtures_count=len(fixtures),
        processed_count=processed,
        selected_count=0,
        ignored_count=ignored,
        done_count=0,
        requests_used=budget.used,
    )


# -------------------------------------------------
# 15:00 JOB – SEÇ + İŞLE (ANA İŞ)
# -------------------------------------------------
def run_job_1500(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    raw = _sort_by_kickoff(repo.list_raw_fixtures(day))

    # 1 request: fixtures refresh (status güncelle)
    if budget.can_consume(1):
        budget.consume(1)
        status_map = _refresh_status_map(ds, day)
        raw = _apply_status(raw, status_map)

    processed = selected = ignored = done = 0

    for m in raw:
        # Seçim limitini "seçilen maç" üzerinden tutuyoruz
        if selected >= cfg.max_matches_per_day:
            break

        if repo.is_done(m.match_id):
            continue

        if _should_ignore_status(m.status):
            repo.mark_ignored(m.match_id, m.status)
            ignored += 1
            continue

        processed += 1

        # --- FT: MS odds şart; varsa score al; done yap
        if _is_ft(m.status):
            # 1 request: odds
            if not budget.can_consume(1):
                break
            budget.consume(1)
            odds = ds.get_ms_odds(m.match_id)

            # MS odds yoksa ele
            if odds is None:
                repo.mark_ignored(m.match_id, "NO_MS_ODDS")
                ignored += 1
                continue

            # seçilmiş maçı yaz
            repo.upsert_match(m)
            repo.save_ms_odds(m.match_id, odds)
            selected += 1

            # 1 request: score
            if budget.can_consume(1):
                budget.consume(1)
                score = ds.get_score(m.match_id)
                if score is not None:
                    repo.save_score(m.match_id, score)
                    repo.mark_done(m.match_id)
                    done += 1
            # score gelmezse 23:00’e bırak (done işaretleme)

            continue

        # --- NS: MS odds varsa seç (snapshot)
        if _is_ns(m.status):
            if not budget.can_consume(1):
                break
            budget.consume(1)
            odds = ds.get_ms_odds(m.match_id)

            if odds is None:
                # odds yoksa ele
                ignored += 1
                continue

            repo.upsert_match(m)
            repo.save_ms_odds(m.match_id, odds)
            selected += 1
            continue

        # Diğer statüler (LIVE/HT vs): burada bir şey yapmıyoruz
        # 23:00 finalize'a kalır.
        continue

    return JobResult(
        day=day,
        job_name="15:00",
        fixtures_count=len(raw),
        processed_count=processed,
        selected_count=selected,
        ignored_count=ignored,
        done_count=done,
        requests_used=budget.used,
    )


# -------------------------------------------------
# 23:00 JOB – FINALIZE + EKSİK TAMAMLAMA
# -------------------------------------------------
def run_job_2300(cfg: Config, ds: DataSource, repo: Repository, day: date) -> JobResult:
    budget = BudgetTracker(limit=cfg.max_daily_requests)

    raw = _sort_by_kickoff(repo.list_raw_fixtures(day))

    # 1 request: fixtures refresh (status güncelle)
    if budget.can_consume(1):
        budget.consume(1)
        status_map = _refresh_status_map(ds, day)
        raw = _apply_status(raw, status_map)

    processed = ignored = done = 0

    for m in raw:
        # 23:00 sadece "seçilmiş" maçlarla ilgilenir:
        # seçilmiş = ms_odds var
        if not repo.has_ms_odds(m.match_id):
            continue

        if repo.is_done(m.match_id):
            continue

        if _should_ignore_status(m.status):
            repo.mark_ignored(m.match_id, m.status)
            ignored += 1
            continue

        processed += 1

        # Odds eksikse tamamla (kural: sadece eksik odds)
        if not repo.has_ms_odds(m.match_id):
            if budget.can_consume(1):
                budget.consume(1)
                odds = ds.get_ms_odds(m.match_id)
                if odds is not None:
                    repo.save_ms_odds(m.match_id, odds)

        # FT olduysa skor tamamla + done
        if _is_ft(m.status):
            if not repo.has_score(m.match_id):
                if budget.can_consume(1):
                    budget.consume(1)
                    score = ds.get_score(m.match_id)
                    if score is not None:
                        repo.save_score(m.match_id, score)

            # Skor varsa done işaretle
            if repo.has_score(m.match_id):
                repo.mark_done(m.match_id)
                done += 1

    return JobResult(
        day=day,
        job_name="23:00",
        fixtures_count=len(raw),
        processed_count=processed,
        selected_count=0,
        ignored_count=ignored,
        done_count=done,
        requests_used=budget.used,
                 )
