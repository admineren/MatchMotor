# app/core/repository.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Optional

from .models import Match, MsOdds, Score


class Repository(ABC):
    """
    DB yazma/okuma katmanı.
    Job motoru sadece bu arayüzü görür.
    """

    @abstractmethod
    def upsert_match(self, match: Match) -> None:
        """Seçilmiş maçı matches tablosuna yazar (idempotent)."""
        raise NotImplementedError

    @abstractmethod
    def save_ms_odds(self, match_id: int, odds: MsOdds) -> None:
        """MS odds kaydeder. Pro aşamada overwrite yok varsayılır."""
        raise NotImplementedError

    @abstractmethod
    def save_score(self, match_id: int, score: Score) -> None:
        """HT/FT skorlarını kademeli kaydeder (overwrite yok)."""
        raise NotImplementedError

    @abstractmethod
    def mark_done(self, match_id: int) -> None:
        """Maçı DONE yapar (kilit)."""
        raise NotImplementedError

    @abstractmethod
    def is_done(self, match_id: int) -> bool:
        """Maç DONE mı? DONE ise dokunma kuralı için."""
        raise NotImplementedError

    @abstractmethod
    def mark_ignored(self, match_id: int, reason: str) -> None:
        """PST/CANC vb. durumlarda ignore işaretler."""
        raise NotImplementedError

    @abstractmethod
    def today_added_count(self, day: date) -> int:
        """Panel: Bugün DB'ye eklenen maç sayısı."""
        raise NotImplementedError

    @abstractmethod
    def total_matches_count(self) -> int:
        """Panel: DB toplam maç sayısı."""
        raise NotImplementedError


@dataclass
class NoOpRepository(Repository):
    """
    Dev/test ortamı: DB yazmaz.
    Job akışını güvenle çalıştırmak için.
    """

    def upsert_match(self, match: Match) -> None:
        return

    def save_ms_odds(self, match_id: int, odds: MsOdds) -> None:
        return

    def save_score(self, match_id: int, score: Score) -> None:
        return

    def mark_done(self, match_id: int) -> None:
        return

    def is_done(self, match_id: int) -> bool:
        return False

    def mark_ignored(self, match_id: int, reason: str) -> None:
        return

    def today_added_count(self, day: date) -> int:
        return 0

    def total_matches_count(self) -> int:
        return 0
