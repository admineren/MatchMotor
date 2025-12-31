# app/core/mock_source.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .datasource import DataSource
from .models import FixtureBundle, Match, MsOdds


class MockSource(DataSource):
    """
    Mock data kaynağı.
    - get_fixtures(day): o güne ait sabit bir fixture listesi döner
    - get_ms_odds_bulk(day): fixture içinden seçili maçlara MS odds döner (bulk)
    """

    def __init__(self) -> None:
        # day -> FixtureBundle cache
        self._cache: Dict[str, FixtureBundle] = {}

    def get_fixtures(self, day: str) -> FixtureBundle:
        """
        day formatı: 'YYYY-MM-DD'
        """
        if day in self._cache:
            return self._cache[day]

        # Mock fixture üret
        matches = self._generate_mock_matches(day)
        bundle = FixtureBundle(day=day, matches=matches)

        self._cache[day] = bundle
        return bundle

    def get_ms_odds_bulk(self, day: str) -> Dict[int, MsOdds]:
        """
        Bulk MS odds.
        DİKKAT: Match üzerinde ms_odds alanı yok; biz map döndürüyoruz:
          {match_id: MsOdds(...)}
        """
        bundle = self.get_fixtures(day)
        odds_map: Dict[int, MsOdds] = {}

        for m in bundle.matches:
            # Her maçta odds olmasın: yaklaşık %35-40’ına odds verelim
            # (gerçeğe benzer: bazı alt liglerde odds yok)
            if (m.match_id % 5) in (0, 2):
                # Basit ama değişken odds üretimi
                home = 1.60 + (m.match_id % 20) * 0.03   # 1.60 - 2.17
                draw = 3.00 + (m.match_id % 15) * 0.04   # 3.00 - 3.56
                away = 3.20 + (m.match_id % 25) * 0.05   # 3.20 - 4.45

                odds_map[m.match_id] = MsOdds(
                    home=round(home, 2),
                    draw=round(draw, 2),
                    away=round(away, 2),
                )

        return odds_map

    # -----------------------
    # internal helpers
    # -----------------------

    def _generate_mock_matches(self, day: str) -> List[Match]:
        """
        jobs.py içinde kullanılan alanlara uygun Match üretir.
        Match modelinde hangi alanlar zorunluysa, onu doldururuz.

        Burada varsayım:
          Match(
            match_id: int,
            kickoff_ts: int,
            status: str,
            home: str,
            away: str,
            league: str
          )
        Eğer senin models.py farklıysa, bana models.py ekran görüntüsü at;
        30 saniyede birebir uyarlarız.
        """

        # Günün başlangıcı (UTC) -> TS
        dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        base_ts = int(dt.timestamp())

        matches = []
