from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from .datasource import DataSource, FixtureBundle
from .models import Match, MsOdds


class MockSource(DataSource):
    """
    DEV/TEST için sahte datasource.
    Amaç: Jobs akışını (15:00 / 23:00) gerçek API olmadan çalıştırmak.

    Not: DataSource abstract olduğu için burada gereken tüm method isimleri mevcut.
    Signature uyuşmazlığı sorun olmasın diye bazı methodlar *args/**kwargs kabul eder.
    """

    def __init__(self, seed: int = 42, fixtures_count: int = 420, ms_odds_ratio: float = 1.0, **kwargs):
        self.seed = seed
        self.fixtures_count = fixtures_count
        self.ms_odds_ratio = ms_odds_ratio
        self._bundle_cache: Dict[str, FixtureBundle] = {}

    # -----------------------------
    # Helpers
    # -----------------------------
    def _day_key(self, day: str) -> str:
        return str(day)

    def _rand(self, n: int) -> int:
        # basit deterministic pseudo-random (telefon ortamında stabil)
        x = (n * 1103515245 + 12345 + self.seed) & 0x7FFFFFFF
        return x

    def _make_match(self, idx: int, kickoff: datetime, status: str) -> Match:
        """
        Match dataclass alanları projende farklı olabilir.
        Bu yüzden annotations üzerinden güvenli kwargs basıyoruz.
        """
        ann = getattr(Match, "__annotations__", {}) or {}
        kwargs: Dict[str, Any] = {}

        # olası alanlar
        candidates = {
            "match_id": idx,
            "id": idx,
            "fixture_id": idx,
            "league": f"MockLeague {self._rand(idx) % 10}",
            "league_name": f"MockLeague {self._rand(idx) % 10}",
            "home": f"Home {idx}",
            "home_team": f"Home {idx}",
            "away": f"Away {idx}",
            "away_team": f"Away {idx}",
            "kickoff": kickoff,
            "kickoff_utc": kickoff,
            "date": kickoff,
            "status": status,
        }

        for k, v in candidates.items():
            if k in ann:
                kwargs[k] = v

        # Eğer annotation yoksa, en azından yaygın isimlerle dene
        if not kwargs:
            # bu blok TypeError yakalayıp farklı ihtimalleri deneyecek
            pass

        # farklı constructor ihtimalleri için denemeler
        try:
            return Match(**kwargs)  # type: ignore
        except TypeError:
            # minimum set dene
            try:
                return Match(id=idx, kickoff=kickoff, status=status)  # type: ignore
            except TypeError:
                try:
                    return Match(match_id=idx, kickoff=kickoff, status=status)  # type: ignore
                except TypeError:
                    # son çare: sadece id
                    return Match(idx)  # type: ignore

    def _make_bundle(self, day: str) -> FixtureBundle:
        key = self._day_key(day)
        if key in self._bundle_cache:
            return self._bundle_cache[key]

        # kickoff dağılımı: gün içine yay
        base = datetime.strptime(f"{day} 00:00", "%Y-%m-%d %H:%M")
        matches: List[Match] = []
        for i in range(1, self.fixtures_count + 1):
            # 0..23 saat arası yay
            hour = (self._rand(i) % 24)
            minute = (self._rand(i + 999) % 60)
            kickoff = base + timedelta(hours=hour, minutes=minute)

            # status üretimi:
            # bir kısmı FT, bir kısmı NS, bir kısmı LIVE gibi
            r = self._rand(i + 555) % 100
            if r < 35:
                status = "FT"
            elif r < 45:
                status = "HT"
            elif r < 55:
                status = "2H"
            else:
                status = "NS"

            matches.append(self._make_match(i, kickoff, status))

        # FixtureBundle alanlarını güvenli doldur
        # FixtureBundle dataclass'ı datasource.py içinde; genelde "day" ve "matches" olur.
        try:
            bundle = FixtureBundle(day=day, matches=matches)  # type: ignore
        except TypeError:
            try:
                bundle = FixtureBundle(matches=matches)  # type: ignore
            except TypeError:
                bundle = FixtureBundle(day, matches)  # type: ignore

        self._bundle_cache[key] = bundle
        return bundle

    # -----------------------------
    # DataSource required methods
    # -----------------------------
    def get_fixtures(self, day: str, *args, **kwargs) -> FixtureBundle:
        return self._make_bundle(day)

    def get_ms_odds_bulk(self, day: str, *args, **kwargs) -> Dict[int, MsOdds]:
        """
        jobs.py line 173: expected Dict[int, MsOdds]
        """
        bundle = self._make_bundle(day)
        odds_map: Dict[int, MsOdds] = {}

        for idx, m in enumerate(getattr(bundle, "matches", []), start=1):
            # match id bul
            mid = None
            for attr in ("match_id", "fixture_id", "id"):
                if hasattr(m, attr):
                    mid = getattr(m, attr)
                    break
            if mid is None:
                mid = idx

            # odds üret (1.20 - 6.00 arası)
            base = (self._rand(int(mid)) % 400) / 100.0
            home = 1.20 + (base % 2.50)
            draw = 2.80 + ((base * 0.7) % 2.20)
            away = 2.00 + ((base * 0.9) % 4.00)

            try:
                odds = MsOdds(home=round(home, 2), draw=round(draw, 2), away=round(away, 2))  # type: ignore
            except TypeError:
                # farklı field isimleri ihtimali
                try:
                    odds = MsOdds(ms1=round(home, 2), ms0=round(draw, 2), ms2=round(away, 2))  # type: ignore
                except TypeError:
                    odds = MsOdds(round(home, 2), round(draw, 2), round(away, 2))  # type: ignore

            odds_map[int(mid)] = odds

        return odds_map

    def get_ms_odds(self, match_id: int, *args, **kwargs) -> Optional[MsOdds]:
        # tek maç odds (bulk'tan da dönebilirdik)
        base = (self._rand(int(match_id)) % 400) / 100.0
        home = 1.20 + (base % 2.50)
        draw = 2.80 + ((base * 0.7) % 2.20)
        away = 2.00 + ((base * 0.9) % 4.00)
        try:
            return MsOdds(home=round(home, 2), draw=round(draw, 2), away=round(away, 2))  # type: ignore
        except TypeError:
            try:
                return MsOdds(ms1=round(home, 2), ms0=round(draw, 2), ms2=round(away, 2))  # type: ignore
            except TypeError:
                return MsOdds(round(home, 2), round(draw, 2), round(away, 2))  # type: ignore

    def get_score(self, match_id: int, *args, **kwargs) -> Optional[dict]:
        """
        jobs.py skor kapatma için çağırıyorsa diye basit skor döndürür.
        (Repo dev ortamında no-op olsa bile job akışı kırılmasın.)
        """
        r = self._rand(int(match_id)) % 100
        if r < 35:
            status = "FT"
        elif r < 45:
            status = "HT"
        elif r < 55:
            status = "2H"
        else:
            status = "NS"

        ht_home = self._rand(int(match_id) + 10) % 3
        ht_away = self._rand(int(match_id) + 20) % 3
        ft_home = ht_home + (self._rand(int(match_id) + 30) % 3 if status == "FT" else 0)
        ft_away = ht_away + (self._rand(int(match_id) + 40) % 3 if status == "FT" else 0)

        return {
            "status": status,
            "ht": (int(ht_home), int(ht_away)),
            "ft": (int(ft_home), int(ft_away)),
                    }
