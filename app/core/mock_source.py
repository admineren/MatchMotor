from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from .datasource import DataSource
from .models import Match, MsOdds


@dataclass
class FixtureBundle:
    day: str
    matches: List[Match]


class MockSource(DataSource):
    """
    DEV/TEST için sahte datasource.
    Amaç: Jobs akışını (15:00 / 23:00) API çağırmadan test etmek.

    Match dataclass imzası:
      match_id, league_id, kickoff_utc(datetime), home_team_id, away_team_id, status
      is_done / is_ignored opsiyonel
    """

    def __init__(self, seed: int = 42, fixtures_count: int = 420):
        self.seed = seed
        self.fixtures_count = fixtures_count
        self._bundle_cache: Dict[str, FixtureBundle] = {}

    # -----------------------------
    # Helpers (deterministic random)
    # -----------------------------
    def _rng(self, n: int) -> int:
        # basit deterministic pseudo-random
        x = (n * 1103515245 + 12345 + self.seed) & 0x7FFFFFFF
        return x

    def _day_base_utc(self, day: str) -> datetime:
        # day format: "YYYY-MM-DD"
        # UTC midnight
        dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt

    def _make_match(self, idx: int, kickoff_utc: datetime, status: str) -> Match:
        league_id = 1 + (idx % 40)
        home_team_id = 1000 + (idx * 2)
        away_team_id = 1000 + (idx * 2) + 1

        return Match(
            match_id=idx,
            league_id=league_id,
            kickoff_utc=kickoff_utc,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            status=status,
            is_done=False,
            is_ignored=False,
        )

    def _make_bundle(self, day: str) -> FixtureBundle:
        if day in self._bundle_cache:
            return self._bundle_cache[day]

        base = self._day_base_utc(day)

        matches: List[Match] = []
        for i in range(1, self.fixtures_count + 1):
            # 0..23 saat dağıt
            hour = self._rng(i) % 24
            minute = self._rng(i + 999) % 60
            kickoff = base + timedelta(hours=hour, minutes=minute)

            # çoğu NS, küçük kısmı PST/CANC karışık
            r = self._rng(i + 202) % 100
            if r < 85:
                status = "NS"
            elif r < 92:
                status = "PST"
            else:
                status = "CANC"

            matches.append(self._make_match(i, kickoff, status))

        bundle = FixtureBundle(day=day, matches=matches)
        self._bundle_cache[day] = bundle
        return bundle

    # -----------------------------
    # DataSource required methods
    # -----------------------------
    def get_fixtures(self, day: str) -> FixtureBundle:
        # jobs.py FixtureBundle bekliyorsa bu isim aynı kalmalı.
        return self._make_bundle(day)

    def get_ms_odds(self, match_id: int):
        return None

    def get_ms_odds_bulk(self, day: str) -> Dict[int, MsOdds]:
        """
        15:00 job burayı çağırıyor.
        match_id -> MsOdds map döndür.
        """
        bundle = self._make_bundle(day)
        odds_map: Dict[int, MsOdds] = {}

        for m in bundle.matches:
            # Odds olmayan lig/maç simülasyonu (PST/CANC genelde odds yok gibi)
            if m.status in ("PST", "CANC"):
                continue

            # 1X2 oranları: deterministic üret
            r1 = (self._rng(m.match_id + 10) % 200) / 100.0  # 0.00..1.99
            r2 = (self._rng(m.match_id + 20) % 200) / 100.0
            r3 = (self._rng(m.match_id + 30) % 200) / 100.0

            home = 1.20 + r1
            draw = 2.40 + r2
            away = 1.60 + r3

            odds_map[m.match_id] = MsOdds(
                home=round(home, 2),
                draw=round(draw, 2),
                away=round(away, 2),
            )

        return odds_map

    def get_score(self, match_id: int) -> Optional[Dict]:
        """
        23:00 job skor kapatırken burayı çağırabilir.
        Basit skor simülasyonu döndürüyoruz.
        """
        # deterministic skor
        hg = self._rng(match_id + 500) % 5
        ag = self._rng(match_id + 800) % 5
        return {"home_goals": int(hg), "away_goals": int(ag), "status": "FT"}
