# app/core/datasource.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Optional, List

from .models import Match, MsOdds, Score


@dataclass(frozen=True)
class FixtureBundle:
    """
    Tek bir 'fixtures' çağrısının çıktısı.
    Aynı bundle içinden:
    - maç listesi (Match)
    - mümkünse skor bilgileri (Score) okunabilir.
    """
    matches: List[Match]


class DataSource(ABC):
    """
    Veri kaynağı arayüzü.
    - MockSource ve ApiSource bunu implement eder.
    Job motoru sadece bu arayüzü görür.
    """

    @abstractmethod
    def get_fixtures(self, day: date) -> FixtureBundle:
        """
        Belirli günün bültenini döndürür (geçici havuz).
        Buradan gelen matches listesi DB'ye direkt yazılmaz,
        önce MS odds filtresi + limit uygulanır.
        """
        raise NotImplementedError

    @abstractmethod
    def get_ms_odds(self, match_id: int) -> Optional[MsOdds]:
        """
        MS (1X2) odds döndürür.
        - Yoksa None.
        - Job tarafında, MS odds eksik olan maçlar için çağrılır.
        """
        raise NotImplementedError

    @abstractmethod
    def get_score(self, match_id: int) -> Optional[Score]:
        """
        Skor bilgisi (HT/FT) döndürür.
        Çoğu API fixtures içinde skor verdiği için,
        ileride bu method opsiyonel kullanılabilir.
        Şimdilik arayüzde dursun.
        """
        raise NotImplementedError
