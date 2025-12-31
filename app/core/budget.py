# app/core/budget.py
from dataclasses import dataclass


@dataclass
class BudgetTracker:
    """
    Günlük request bütçesini takip eder.
    - Sistem limiti: Config.max_daily_requests (örn 600)
    - API hard limit: Config.hard_api_limit (örn 650) sadece bilgi/tampon (burada kullanmıyoruz)
    """
    limit: int
    used: int = 0

    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def can_consume(self, n: int = 1) -> bool:
        if n < 0:
            raise ValueError("n must be >= 0")
        return (self.used + n) <= self.limit

    def consume(self, n: int = 1) -> None:
        """
        Bütçeden n request harcar. Limit aşılırsa hata fırlatır.
        Job tarafında kullanmadan önce can_consume() ile kontrol edilmesi beklenir.
        """
        if n < 0:
            raise ValueError("n must be >= 0")
        if not self.can_consume(n):
            raise RuntimeError(
                f"Daily request budget exceeded: used={self.used}, "
                f"trying={n}, limit={self.limit}"
            )
        self.used += n
