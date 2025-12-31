# app/core/mock_source.py

from __future__ import annotations

import random
from datetime import datetime, date, time, timedelta

from .datasource import DataSource
from .models import Match, FixtureBundle, MsOdds


class MockSource(DataSource):
    """
    Mock DataSource (DEV/TEST):
    - Gerçek API çağrısı yok.
    - jobs.py akışını test etmek için deterministik/tekrar üretilebilir fixture + MS odds üretir.
    """

    def __init__(self, seed: int = 42, fixture_count: int = 420):
        self._seed = seed
        self._fixture_count = fixture_count
        self._generated = {}  # day -> FixtureBundle

    # ---------------------------
    # Public API (DataSource)
    # ---------------------------

    def get_fixtures(self, day: date) -> FixtureBundle:
        # Aynı gün aynı veriyi versin diye cache + sabit seed
        if day in self._generated:
            return self._generated[day]

        rng = random.Random(self._seed + int(day.strftime("%Y%m%d")))

        # Gün içi maç saatlerini yay: 00:00 - 23:59 arası (TR mantığı gibi)
        matches = []
        base_dt = datetime.combine(day, time(0, 0))

        for i in range(self._fixture_count):
            # kickoff'u gün içine dağıt
            minute_of_day = rng.randint(0, 23 * 60 + 59)
            kickoff = base_dt + timedelta(minutes=minute_of_day)

            # status üret: DONE/NS/1H/HT/2H vb.
            # job mantığını test etmek için bir kısmını DONE yapıyoruz.
            roll = rng.random()
            if roll < 0.35:
                status = "FT"        # bitti
            elif roll < 0.45:
                status = "NS"        # başlamadı
            elif roll < 0.60:
                status = "HT"        # devre
            else:
                status = "LIVE"      # oynanıyor

            # MS odds var/yok:
            # büyük kısmında var; bir kısmında yok (ignored sayısı artsın diye)
            has_ms_odds = (rng.random() < 0.55)

            m = Match(
                id=100000 + i,
                kickoff_ts=int(kickoff.timestamp()),
                status=status,
                has_ms_odds=has_ms_odds,
            )
            matches.append(m)

        bundle = FixtureBundle(matches=matches)
        self._generated[day] = bundle
        return bundle

    def get_ms_odds_bulk(self, day: date):
        """
        jobs.py 23:00 tarafında bulk odds çekiyor.
        Burada: fixture listesi içinden has_ms_odds=True olanlara MsOdds üretip map döndürüyoruz.
        """
        bundle = self.get_fixtures(day)
        rng = random.Random(self._seed + 999 + int(day.strftime("%Y%m%d")))

        odds_map = {}
        for m in bundle.matches:
            if not getattr(m, "has_ms_odds", False):
                continue

            # Basit ama mantıklı oranlar üretelim (1 / X / 2)
            # 1.20 - 6.50 bandı
            o1 = round(rng.uniform(1.20, 3.20), 2)
            ox = round(rng.uniform(2.60, 4.80), 2)
            o2 = round(rng.uniform(1.60, 6.50), 2)

            odds_map[m.id] = MsOdds(home=o1, draw=ox, away=o2)

        return odds_map
