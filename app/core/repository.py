from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import List

from .models import Match, MsOdds, Score


class Repository(ABC):
    """
    DB yazma/okuma katmanı.
    Job motoru sadece bu arayüzü görür.
    """

    # ---------- RAW (STAGING) ----------
    @abstractmethod
    def save_raw_fixture(self, match: Match) -> None:
        """01:00 job: ham fixture havuzuna yazar."""
        raise NotImplementedError

    @abstractmethod
    def list_raw_fixtures(self, day: date) -> List[Match]:
        """15:00 job: günün ham fixtures listesini döner."""
        raise NotImplementedError

    # ---------- SELECTED MATCHES ----------
    @abstractmethod
    def upsert_match(self, match: Match) -> None:
        """Seçilmiş maçı matches tablosuna yazar (idempotent)."""
        raise NotImplementedError

    # ---------- ODDS / SCORE ----------
    @abstractmethod
    def save_ms_odds(self, match_id: int, odds: MsOdds) -> None:
        """MS odds kaydeder."""
        raise NotImplementedError

    @abstractmethod
    def has_ms_odds(self, match_id: int) -> bool:
        """Bu maç için MS odds var mı?"""
        raise NotImplementedError

    @abstractmethod
    def save_score(self, match_id: int, score: Score) -> None:
        """HT/FT skorlarını kademeli kaydeder."""
        raise NotImplementedError

    @abstractmethod
    def has_score(self, match_id: int) -> bool:
        """Bu maç için skor var mı?"""
        raise NotImplementedError

    # ---------- STATE ----------
    @abstractmethod
    def mark_done(self, match_id: int) -> None:
        """Maçı DONE yapar (kilit)."""
        raise NotImplementedError

    @abstractmethod
    def is_done(self, match_id: int) -> bool:
        """Maç DONE mı? DONE ise dokunma."""
        raise NotImplementedError

    @abstractmethod
    def mark_ignored(self, match_id: int, reason: str) -> None:
        """PST/CANC vb. durumlarda ignore işaretler."""
        raise NotImplementedError

    # ---------- PANEL ----------
    @abstractmethod
    def today_added_count(self, day: date) -> int:
        """Panel: Bugün DB'ye eklenen maç sayısı."""
        raise NotImplementedError

    @abstractmethod
    def total_matches_count(self) -> int:
        """Panel: DB toplam maç sayısı."""
        raise NotImplementedError


# -------------------------------------------------
# DEV / TEST REPOSITORY (NO-OP)
# -------------------------------------------------
@dataclass
class NoOpRepository(Repository):
    """
    Dev/test ortamı: DB yazmaz.
    Job akışını güvenle çalıştırmak için.
    """

    # RAW
    def save_raw_fixture(self, match: Match) -> None:
        return

    def list_raw_fixtures(self, day: date) -> list[Match]:
        return []

    # SELECTED
    def upsert_match(self, match: Match) -> None:
        return

    # ODDS / SCORE
    def save_ms_odds(self, match_id: int, odds: MsOdds) -> None:
        return

    def has_ms_odds(self, match_id: int) -> bool:
        return False

    def save_score(self, match_id: int, score: Score) -> None:
        return

    def has_score(self, match_id: int) -> bool:
        return False

    # STATE
    def mark_done(self, match_id: int) -> None:
        return

    def is_done(self, match_id: int) -> bool:
        return False

    def mark_ignored(self, match_id: int, reason: str) -> None:
        return

    # PANEL
    def today_added_count(self, day: date) -> int:
        return 0

    def total_matches_count(self) -> int:
        return 0
