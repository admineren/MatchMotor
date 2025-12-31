# app/core/in_memory_repository.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Set

from .models import Match, MsOdds, Score
from .repository import Repository


@dataclass
class InMemoryRepository(Repository):
    """
    RAM üzerinde çalışan repository.
    Dev/test için idealdir: job akışını gerçek gibi görürsün.
    """

    # raw fixtures: day -> match_id -> Match
    _raw: Dict[date, Dict[int, Match]] = field(default_factory=dict)

    # selected matches: match_id -> Match
    _selected: Dict[int, Match] = field(default_factory=dict)

    # odds/scores
    _ms_odds: Dict[int, MsOdds] = field(default_factory=dict)
    _scores: Dict[int, Score] = field(default_factory=dict)

    # state
    _done: Set[int] = field(default_factory=set)
    _ignored: Dict[int, str] = field(default_factory=dict)

    # panel bookkeeping
    _day_added: Dict[date, Set[int]] = field(default_factory=dict)

    # ---------- RAW (STAGING) ----------
    def save_raw_fixture(self, match: Match) -> None:
        d = match.kickoff_utc.date()  # UTC günü
        self._raw.setdefault(d, {})
        self._raw[d][match.match_id] = match

    def list_raw_fixtures(self, day: date) -> List[Match]:
        return list(self._raw.get(day, {}).values())

    # ---------- SELECTED ----------
    def upsert_match(self, match: Match) -> None:
        self._selected[match.match_id] = match
        d = match.kickoff_utc.date()
        self._day_added.setdefault(d, set())
        self._day_added[d].add(match.match_id)

    def list_selected_matches(self, day: date) -> List[Match]:
        return [m for m in self._selected.values() if m.kickoff_utc.date() == day]

    # ---------- ODDS / SCORE ----------
    def save_ms_odds(self, match_id: int, odds: MsOdds) -> None:
        self._ms_odds[match_id] = odds

    def has_ms_odds(self, match_id: int) -> bool:
        return match_id in self._ms_odds

    def save_score(self, match_id: int, score: Score) -> None:
        self._scores[match_id] = score

    def has_score(self, match_id: int) -> bool:
        return match_id in self._scores

    # ---------- STATE ----------
    def mark_done(self, match_id: int) -> None:
        self._done.add(match_id)

    def is_done(self, match_id: int) -> bool:
        return match_id in self._done

    def mark_ignored(self, match_id: int, reason: str) -> None:
        self._ignored[match_id] = reason

    # ---------- PANEL ----------
    def today_added_count(self, day: date) -> int:
        return len(self._day_added.get(day, set()))

    def total_matches_count(self) -> int:
        return len(self._selected)
