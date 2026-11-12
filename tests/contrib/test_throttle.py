import asyncio
import pytest

from unittest.mock import patch

from amqtt.contrib.throttle.limiters import RateLimiter, TokenBucketRateLimiter


class MyTestRateLimiter(RateLimiter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate = 10

    def allow(self, client_id: str, *args, **kwargs) -> bool:
        return True


@pytest.mark.asyncio
async def test_window_limiter_single_client_allow():
    """0 seconds - no tokens in bucket
4 seconds - 10 tokens in bucket
8 seconds - 20 tokens in bucket
client1, sent 20 messages, all would be approved
12 seconds - 20 tokens in bucket
16 seconds - 20 tokens in bucket
20 seconds - 20 tokens in bucket
"""



    rate_limiter = MyTestRateLimiter()

    for i in range(10):
        assert rate_limiter.allow("client1")
        await asyncio.sleep(0.1)

@pytest.mark.asyncio
async def test_window_limiter_multiple_client_allow():
    """
    0 seconds - no tokens in bucket
4 seconds - 10 tokens in bucket
8 seconds - 20 tokens in bucket
client1, sent 20 messages, all would be approved
client2, sent 20 messages, all would be approved
12 seconds - 20 tokens in bucket
16 seconds - 20 tokens in bucket
20 seconds - 20 tokens in bucket
    """

    rate_limiter = WindowRateLimiter(...)

    for i in range(10):
        assert rate_limiter.allow("client1")
        await asyncio.sleep(0.1)
        assert rate_limiter.allow("client2")
        await asyncio.sleep(0.1)

# additional test scenarios...
"""
0 seconds - 0 messages
4 seconds - 0 messages
8 seconds - 10 messages get sent, then 10 messages get denied
12 seconds - 0 messages
16 seconds - 0 messages
20 seconds - 0 messages
"""

def test_token_bucket_initial_burst():
    """The limiter allows an initial burst of messages up to max_tokens."""
    limiter = TokenBucketRateLimiter(10, 4, max_tokens=20)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic", return_value=0):
        for _ in range(20):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False


def test_token_bucket_denied_messages():
    """Messages are denied when the bucket is empty."""
    limiter = TokenBucketRateLimiter(10, 4, max_tokens=10)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic", return_value=0):
        for _ in range(10):
            assert limiter.allow("client1") is True

        for _ in range(10):
            assert limiter.allow("client1") is False


def test_token_bucket_refill():
    """Ten tokens replenish over four seconds."""
    limiter = TokenBucketRateLimiter(10, 4, max_tokens=10)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic") as clock:
        clock.return_value = 0

        for _ in range(10):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False

        # Two seconds replenish five tokens.
        clock.return_value = 2

        for _ in range(5):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False

        # Another two seconds replenish five more.
        clock.return_value = 4

        for _ in range(5):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False


def test_token_bucket_max_capacity():
    """An idle bucket never exceeds max_tokens."""
    limiter = TokenBucketRateLimiter(10, 4, max_tokens=20)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic") as clock:
        clock.return_value = 0

        for _ in range(20):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False

        # Even after 100 seconds, only 20 tokens are available.
        clock.return_value = 100

        for _ in range(20):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False


def test_token_bucket_multiple_clients():
    """Each client maintains an independent token bucket."""
    limiter = TokenBucketRateLimiter(10, 4, max_tokens=10)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic", return_value=0):
        for _ in range(10):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False

        # Client 2 is unaffected by Client 1.
        for _ in range(10):
            assert limiter.allow("client2") is True

        assert limiter.allow("client2") is False


def test_token_bucket_fractional_refill():
    """Fractional tokens accumulate until one token is available."""
    limiter = TokenBucketRateLimiter(10, 4, max_tokens=10)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic") as clock:
        clock.return_value = 0

        for _ in range(10):
            assert limiter.allow("client1") is True

        # At 2.5 tokens/second, 0.2 seconds adds 0.5 tokens.
        clock.return_value = 0.2
        assert limiter.allow("client1") is False

        # At 0.4 seconds, one full token has accumulated.
        clock.return_value = 0.4
        assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False


def test_token_bucket_different_configuration():
    """The limiter supports different message limits and time windows."""
    limiter = TokenBucketRateLimiter(5, 2, max_tokens=5)

    with patch("amqtt.contrib.throttle.limiters.time.monotonic") as clock:
        clock.return_value = 0

        for _ in range(5):
            assert limiter.allow("client1") is True

        assert limiter.allow("client1") is False

        # Five tokens per two seconds means 2.5 tokens/second.
        clock.return_value = 0.8

        assert limiter.allow("client1") is True
        assert limiter.allow("client1") is True
        assert limiter.allow("client1") is False