# app/core/config.py
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """
    Sistemin tüm davranışını kod değiştirmeden yönetmek için tek kaynak.
    Pro -> Ultra geçişte sadece buradaki sayılar değişecek.
    """

    # Plan güvenli çalışma limiti (sistem kendi kendine burada durur)
    max_daily_requests: int = 600

    # API'nin gerçek üst limiti (emniyet kemeri / bilgi amaçlı)
    hard_api_limit: int = 650

    # Günlük DB'ye alınabilecek maksimum maç sayısı
    max_matches_per_day: int = 500

    # Job saatleri (TR) - şimdilik referans olarak burada dursun
    job_time_1: str = "15:00"
    job_time_2: str = "23:00"
