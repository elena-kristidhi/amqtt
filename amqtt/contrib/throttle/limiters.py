import time
from dataclasses import dataclass


class RateLimiter:
    """Base class for rate limiters"""

    def __init__(self, message_limit: int, time_window: int):
        self.message_limit = message_limit
        self.time_window = time_window

    def allow(self, client_id: str, *args, **kwargs ) -> bool:
        """Returns false if the client is not allowed to send a message, otherwise true"""
        raise NotImplementedError("please create a subclass")

@dataclass
class Bucket:
    """Store the current token count and last update time."""
    tokens: float
    last_updated: float


class TokenBucketRateLimiter(RateLimiter):
    """Limits the rate at which messages can be published within a configurable time window"""
    def __init__(self, message_limit: int,time_window: float, max_tokens: int | None = None):

        super().__init__(message_limit, time_window)

        self.max_tokens = (
            max_tokens if max_tokens is not None else message_limit
        )

        if message_limit <= 0 or time_window <= 0:
            raise ValueError("Rate limit and time window must be positive")

        if self.max_tokens <= 0:
            raise ValueError("Maximum tokens must be positive")

        self.refill_rate = message_limit / time_window

        self.buckets: dict[str, Bucket] = {}

    def allow(self, client_id: str, *args, **kwargs) -> bool:
        now = time.monotonic()

        if client_id not in self.buckets:
            self.buckets[client_id] = Bucket(
                tokens=float(self.max_tokens),
                last_updated=now,
            )

        bucket = self.buckets[client_id]

        elapsed = now - bucket.last_updated

        bucket.tokens = min(
            self.max_tokens,
            bucket.tokens + elapsed * self.refill_rate,
        )

        bucket.last_updated = now

        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return True

        return False

class WindowRateLimiter(RateLimiter):
    """Limits the rate at which messages can be published within a sliding time window"""

    def __init__(self, message_limit: int, time_window: float):
        super().__init__(message_limit, time_window)

        if message_limit <= 0 or time_window <= 0:
            raise ValueError("Rate limit and time window must be positive")

        self.timestamps: dict[str, list[float]] = {}

    def allow(self, client_id: str, *args, **kwargs) -> bool:
        now = time.monotonic()

        if client_id not in self.timestamps:
            self.timestamps[client_id] = []

        client_timestamps = self.timestamps[client_id]

        while client_timestamps and now - client_timestamps[0] >= self.time_window:
            client_timestamps.pop(0)

        if len(client_timestamps) >= self.message_limit:
            return False

        client_timestamps.append(now)
        return True
