# app/core/mock_source.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Optional, List, Any

from .datasource import DataSource, FixtureBundle
from .models import Match, MsOdds, Score


@dataclass(frozen=True)
class _MockDayCache:
    matches: List[Match]
    status_by_id: Dict[int, str]
    odds_by_id: Dict[int, MsOdds]
    score_by_id: Dict[int, Score]


class MockSource(DataSource):
    """
    Sıfırdan stabil Mock datasource.

    - get_fixtures(day): deterministik fixture listesi üretir (Match listesi)
    - get_ms_odds(match_id): odds varsa MsOdds döner, yoksa None
    - get_score(match_id): sadece FT olanlar için Score döner, yoksa None

    Notlar:
    - day parametresi date tipidir (DataSource sözleşmesi).
    - kickoff_utc timezone-aware (UTC) üretilir.
    - ms_odds_ratio ile "odds varmış gibi davranma oranı" ayarlanabilir.
    - Constructor **fazladan keyword** alsa bile patlamaz (**kwargs).
    """

    def __init__(
        self,
        seed: int = 42,
        fixtures_count: int = 420,
        ms_odds_ratio: float = 0.85,
        ft_ratio: float = 0.20,
        ignore_ratio: float = 0.03,
        **kwargs: Any,
    ):
        self.seed = int(seed)
        self.fixtures_count = int(fixtures_count)
        self.ms_odds_ratio = float(ms_odds_ratio)
        self.ft_ratio = float(ft_ratio)
        self.ignore_ratio = float(ignore_ratio)

        self._cache_by_day: Dict[date, _MockDayCache] = {}
        self._day_by_match_id: Dict[int, date] = {}

    # -------------------------
    # Deterministic pseudo-rng
    # -------------------------
    def _rng(self, x: int) -> int:
        # basit deterministik LCG
        return (x * 1103515245 + 12345 + self.seed) & 0x7FFFFFFF

    def _rand01(self, x: int) -> float:
        return (self._rng(x) % 10_000) / 10_000.0

    def _kickoff_for(self, d: date, i: int) -> datetime:
        # gün içine dağıt: 00:00 - 23:59 UTC
        base = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
        hour = self._rng(i + 100) % 24
        minute = self._rng(i + 200) % 60
        return base + timedelta(hours=hour, minutes=minute)

    def _make_day_cache(self, d: date) -> _MockDayCache:
        if d in self._cache_by_day:
            return self._cache_by_day[d]

        matches: List[Match] = []
        status_by_id: Dict[int, str] = {}
        odds_by_id: Dict[int, MsOdds] = {}
        score_by_id: Dict[int, Score] = {}

        # Match ID'leri gün bazlı çakışmasın diye date'ten offset üret
        # (yeterince deterministik ve stabil)
        day_key = int(d.strftime("%Y%m%d"))
        base_id = (day_key % 1_000_000) * 10_000  # 20251231 -> 1231xxxx gibi

        for n in range(1, self.fixtures_count + 1):
            match_id = base_id + n
            league_id = 1 + (n % 40)
            home_team_id = 1000 + (n * 2)
            away_team_id = 1000 + (n * 2) + 1
            kickoff_utc = self._kickoff_for(d, n)

            r = self._rand01(match_id)

            # ignore (PST/CANC)
            if r < self.ignore_ratio:
                status = "PST" if (self._rng(match_id) % 2 == 0) else "CANC"
            # FT
            elif r < self.ignore_ratio + self.ft_ratio:
                status = "FT"
            # default NS
            else:
                status = "NS"

            m = Match(
                match_id=match_id,
                league_id=league_id,
                kickoff_utc=kickoff_utc,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                status=status,
                is_done=False,
                is_ignored=False,
            )
            matches.append(m)
            status_by_id[match_id] = status
            self._day_by_match_id[match_id] = d

            # Odds üretimi (oranı ms_odds_ratio kadar)
            if status not in ("PST", "CANC"):
                if self._rand01(match_id + 999) < self.ms_odds_ratio:
                    # deterministik 1X2
                    a = 1.20 + (self._rng(match_id + 10) % 200) / 100.0
                    b = 2.40 + (self._rng(match_id + 20) % 200) / 100.0
                    c = 1.60 + (self._rng(match_id + 30) % 200) / 100.0

                    odds_by_id[match_id] = MsOdds(
                        home=round(a, 2),
                        draw=round(b, 2),
                        away=round(c, 2),
                        taken_at=datetime.now(timezone.utc),
                    )

            # Score üretimi (sadece FT)
            if status == "FT":
                hg = self._rng(match_id + 500) % 5
                ag = self._rng(match_id + 800) % 5
                # HT basit türet
                ht_h = min(int(hg), int(self._rng(match_id + 1500) % 3))
                ht_a = min(int(ag), int(self._rng(match_id + 1800) % 3))
                score_by_id[match_id] = Score(
                    ht_home=ht_h,
                    ht_away=ht_a,
                    ft_home=int(hg),
                    ft_away=int(ag),
                    went_extra_time=False,
                    went_penalties=False,
                )

        cache = _MockDayCache(
            matches=matches,
            status_by_id=status_by_id,
            odds_by_id=odds_by_id,
            score_by_id=score_by_id,
        )
        self._cache_by_day[d] = cache
        return cache

    # -------------------------
    # DataSource implementation
    # -------------------------
    def get_fixtures(self, day: date) -> FixtureBundle:
        cache = self._make_day_cache(day)
        return FixtureBundle(matches=cache.matches)

    def get_ms_odds(self, match_id: int) -> Optional[MsOdds]:
        d = self._day_by_match_id.get(match_id)
        if d is None:
            return None
        cache = self._make_day_cache(d)
        return cache.odds_by_id.get(match_id)

    def get_score(self, match_id: int) -> Optional[Score]:
        d = self._day_by_match_id.get(match_id)
        if d is None:
            return None
        cache = self._make_day_cache(d)
        return cache.score_by_id.get(match_id)
