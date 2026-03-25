import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable, Awaitable, Optional
import redis.asyncio as aioredis
from pathlib import Path


# ---------------------------------------------------------------------
# Init globals
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class RateLimitRule:
    """
    Represent a rate limit rule
    """
    limit: int
    window_ms: int



API_RULES: dict[str, list[RateLimitRule]] = {
    "A": [
        RateLimitRule(limit=5, window_ms=1_000),
        RateLimitRule(limit=500, window_ms=60_000),
        RateLimitRule(limit=16_500, window_ms=3_600_000),
    ],
}

RETRY_INTERVAL_S = 0.05

with open(Path(__file__).parent/"redis_logic.lua", 'r') as file:
    lua_text = file.read()

REDIS_SCRIPT = lua_text

POOL: Optional[aioredis.Redis] = None


# ---------------------------------------------------------------------
# Setup redis pool interface
# ---------------------------------------------------------------------

def get_redis_pool() -> aioredis.Redis:
    global POOL
    if POOL is None:
        POOL = aioredis.Redis(
            host="localhost", port=6379,
            decode_responses=True, max_connections=20,
        )
    return POOL


async def close_redis_pool() -> None:
    global POOL
    if POOL is not None:
        await POOL.aclose()
        POOL = None


# ---------------------------------------------------------------------
# Call API function
# ---------------------------------------------------------------------

async def call_api_with_rate_limit(
    api: str,
    call_api: Callable[..., Awaitable[Any]],
    *args: Any,
    block: bool = False,
    max_wait_s: float = 30.0,
    **kwargs: Any,
) -> Any:
    if api not in API_RULES:
        raise ValueError(f"Unknown API: {api!r}")

    rules = API_RULES[api]
    r = get_redis_pool()

    # Build keys for number of tokens and last refill per rule
    keys = []
    for i in range(len(rules)):
        prefix = f"ratelimit:{api}:{i}"
        keys.append(f"{prefix}:tokens")
        keys.append(f"{prefix}:last_refill")

    # Build api ruleset
    rule_argv = []
    for rule in rules:
        rule_argv.extend([rule.limit, rule.window_ms])

    deadline = time.monotonic() + max_wait_s

    # Try to make API call, keep trying if block for max_wait_s before giving up
    while True:
        now_ms = int(time.time() * 1000)

        wait_ms = int(await r.eval( # type: ignore[misc]
            REDIS_SCRIPT,
            len(keys),
            *keys,
            now_ms,
            *rule_argv,
        ))

        # Call API if tokens available
        if wait_ms == 0:
            return await call_api(*args, **kwargs)

        # Fail call if tokens are missing and not block
        if not block:
            return None

        # Keep trying if block for max_wait_s
        wait_s = min(wait_ms / 1000, RETRY_INTERVAL_S)
        if time.monotonic() + wait_s > deadline:
            return None
        await asyncio.sleep(wait_s)


# ---------------------------------------------------------------------
# Demo - MADE BY JARVIS
# ---------------------------------------------------------------------

async def _demo():
    async def fake_api(msg: str) -> str:
        return f"OK: {msg}"
    print("--- TEST-DEMO COURTESY OF JARVIS ---\n")
    print("--- 8 rapid calls (limit=5/s, expect 5 OK then 3 limited) ---")
    for i in range(8):
        result = await call_api_with_rate_limit("A", fake_api, f"#{i}", block=False)
        print(f"  [{i}] {result or 'RATE LIMITED'}")

    print("\n--- sleep 1.1s ---")
    await asyncio.sleep(1.1)

    print("\n--- 3 more calls (tokens refilled) ---")
    for i in range(3):
        result = await call_api_with_rate_limit("A", fake_api, f"after-sleep-{i}", block=False)
        print(f"  [{i}] {result or 'RATE LIMITED'}")

    print("\n--- blocking call when exhausted ---")
    # Drain remaining tokens
    for _ in range(5):
        await call_api_with_rate_limit("A", fake_api, "drain", block=False)
    result = await call_api_with_rate_limit("A", fake_api, "waited", block=True, max_wait_s=3)
    print(f"  blocking result: {result}")

    await close_redis_pool()


if __name__ == "__main__":
    asyncio.run(_demo())
