# app/core/models.py
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


# -----------------------
# Match (temel maç kaydı)
# -----------------------
@dataclass
class Match:
    match_id: int
    league_id: int
    kickoff_utc: datetime
    home_team_id: int
    away_team_id: int
    status: str  # NS, HT, FT, PST, CANC, etc.

    is_done: bool = False
    is_ignored: bool = False


# -----------------------
# MS Odds (1X2)
# -----------------------
@dataclass
class MsOdds:
    home: float
    draw: float
    away: float
    taken_at: datetime


# -----------------------
# Score (HT + FT)
# -----------------------
@dataclass
class Score:
    ht_home: Optional[int] = None
    ht_away: Optional[int] = None
    ft_home: Optional[int] = None
    ft_away: Optional[int] = None

    went_extra_time: bool = False
    went_penalties: bool = False
