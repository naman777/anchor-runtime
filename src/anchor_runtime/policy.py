"""Persisted execution policies for workflow nodes."""

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: timedelta = timedelta(seconds=1)
    max_delay: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        if not timedelta(0) <= self.base_delay <= self.max_delay:
            raise ValueError("Retry delays must be nonnegative and ordered")

    def delay(self, attempt: int, sample: float) -> timedelta:
        cap = min(
            self.max_delay.total_seconds(),
            self.base_delay.total_seconds() * 2 ** (attempt - 1),
        )
        return timedelta(seconds=cap * sample)


class RetryableError(Exception):
    """A handler or adapter explicitly classified a failure as transient."""

    def __init__(self, message: str = "", *, retry_after: timedelta = timedelta(0)):
        super().__init__(message)
        if retry_after < timedelta(0):
            raise ValueError("Retry-After must be nonnegative")
        self.retry_after = retry_after
