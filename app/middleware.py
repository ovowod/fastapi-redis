import time

from fastapi import Request
from fastapi.responses import JSONResponse


LIMIT = 5
WINDOW = 60


async def rate_limit_middleware(request: Request, call_next):
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis
    user_identifier = request.client.host if request.client else "127.0.0.1"

    # 0 ~ 59초 사이의 요청은 모두 동일한 식별자
    current_minute = int(time.time() // WINDOW)
    cache_key = f"rate_limit:{user_identifier}:{current_minute}"

    count = await rd.incr(cache_key)

    if count == 1:
        await rd.expire(cache_key, WINDOW)

    if count > LIMIT:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "detail": f"Rate limit exceeded: {LIMIT} requests per minute",
                "retry_after": f"{WINDOW - (int(time.time()) % WINDOW)}",
            },
        )

    return await call_next(request)
