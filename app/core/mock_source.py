# app/core/mock_source.py
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from typing import Optional, List, Dict

from .datasource import DataSource, FixtureBundle
from .models import Match, MsOdds, Score


@dataclass
class MockSource(DataSource):
    """
    Gerçek API olmadan job motorunu test etmek için sahte veri üretir.

    - fixtures_count: bir günde üretilecek toplam maç sayısı
    - ms_odds_ratio: maçların yüzde kaçında MS odds var (0.0 - 1.0)
    - status_weights: NS/HT/FT/PST/CANC dağılımı
    - seed: aynı gün aynı çıktıyı üretmek için
    """
    fixtures_count: int = 420
    ms_odds_ratio: float = 0.6
    seed: int = 42

    # Basit status dağılımı (toplamı 1.0 olmalı)
    status_weights: Dict[str, float] = None

    def __post_init__(self) -> None:
        if self.status_weights is None:
            self.status_weights = {
                "NS": 0.50,
                "HT": 0.10,
                "FT": 0.35,
                "PST": 0.03,
                "CANC": 0.02,
            }

    def get_fixtures(self, day: date) -> FixtureBundle:
        rng = random.Random(self._day_seed(day))

        # Gün boyunca (UTC) rastgele kick-off saatleri üretelim
        base = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)

        matches: List[Match] = []
        for i in range(self.fixtures_count):
            match_id = self._make_match_id(day, i)
            kickoff = base + timedelta(minutes=rng.randint(0, 23 * 60 + 59))

            status = self._pick_status(rng)

            m = Match(
                match_id=match_id,
                league_id=rng.randint(1, 2000),
                kickoff_utc=kickoff,
                home_team_id=rng.randint(1, 50000),
                away_team_id=rng.randint(1, 50000),
                status=status,
            )
            matches.append(m)

        return FixtureBundle(matches=matches)

    def get_ms_odds(self, match_id: int) -> Optional[MsOdds]:
        # Match ID üzerinden deterministik karar verelim (aynı match_id aynı sonucu verir)
        rng = random.Random(match_id + self.seed)

        has_odds = rng.random() < self.ms_odds_ratio
        if not has_odds:
            return None

    def get_ms_odds_bulk(self, day):
        """
        Mock: Günlük MS odds toplu snapshot.
        - 1 request simüle eder
        - match_id -> MsOdds dict döndürür
        """
        odds_map = {}

        for m in self.fixtures.matches:
            if m.ms_odds is not None:
                odds_map[m.match_id] = m.ms_odds
                
                return odds_map

        # Gerçekçi bir 1X2 odds üretimi (tam bilimsel değil, test için yeterli)
        home = round(rng.uniform(1.20, 4.50), 2)
        draw = round(rng.uniform(2.80, 5.50), 2)
        away = round(rng.uniform(1.20, 6.50), 2)

        # Zaman damgası (şimdilik now)
        return MsOdds(home=home, draw=draw, away=away, taken_at=datetime.now(timezone.utc))

    def get_score(self, match_id: int) -> Optional[Score]:
        # Basit deterministik skor üretimi (FT/HT için kullanılabilir)
        rng = random.Random(match_id + 999 + self.seed)

        ht_home = rng.randint(0, 2)
        ht_away = rng.randint(0, 2)
        ft_home = ht_home + rng.randint(0, 3)
        ft_away = ht_away + rng.randint(0, 3)

        return Score(
            ht_home=ht_home,
            ht_away=ht_away,
            ft_home=ft_home,
            ft_away=ft_away,
            went_extra_time=False,
            went_penalties=False,
        )

    # -------- helpers --------

    def _day_seed(self, day: date) -> int:
        # aynı gün için aynı seed
        return int(f"{day.year:04d}{day.month:02d}{day.day:02d}") + self.seed

    def _make_match_id(self, day: date, idx: int) -> int:
        # gün + index'ten deterministik match_id
        return int(f"{day.year%100:02d}{day.month:02d}{day.day:02d}{idx:04d}")

    def _pick_status(self, rng: random.Random) -> str:
        r = rng.random()
        acc = 0.0
        for status, w in self.status_weights.items():
            acc += w
            if r <= acc:
                return status
        return "NS"
