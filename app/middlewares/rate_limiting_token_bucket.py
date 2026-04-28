import time

from fastapi import Request
from fastapi.responses import JSONResponse

CAPACITY = 10
REFILL_RATE = 0.1


async def rate_limit_token_bucket(request: Request, call_next):
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis
    user_identifier = request.client.host if request.client else "127.0.0.1"
    cache_key = f"rate_limit:{user_identifier}"

    current_time = time.time()

    stored_tokens = await rd.hget(cache_key, "tokens")
    last_refill = await rd.hget(cache_key, "last_refill")

    if stored_tokens is None or last_refill is None:
        tokens = CAPACITY
    else:
        elapsed = current_time - float(last_refill)
        tokens = min(CAPACITY, float(stored_tokens) + elapsed * REFILL_RATE)

    if tokens < 1:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "detail": f"Rate limit exceeded",
            },
        )

    await rd.hset(cache_key, mapping={"tokens": tokens - 1, "last_refill": current_time})
    await rd.expire(cache_key, CAPACITY + 5)

    return await call_next(request)
