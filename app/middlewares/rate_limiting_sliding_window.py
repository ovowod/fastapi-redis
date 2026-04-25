import time
import uuid

from fastapi import Request
from fastapi.responses import JSONResponse


LIMIT = 5
WINDOW = 60


async def rate_limit_sliding_window(request: Request, call_next):
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis
    user_identifier = request.client.host if request.client else "127.0.0.1"
    cache_key = f"rate_limit:{user_identifier}"

    current_time = time.time()

    # 현재 시간 - WINDOW 보다 오래된 요청 정리
    await rd.zremrangebyscore(cache_key, 0, current_time - WINDOW)

    # 개수 확인
    current_count = await rd.zcard(cache_key)

    if current_count >= LIMIT:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "detail": f"Rate limit exceeded",
            },
        )

    # 제한을 넘지 않았을 때만 타임스탬프 기록
    member = f"{current_time}-{uuid.uuid4().hex[:6]}"
    await rd.zadd(cache_key, {member: current_time})
    await rd.expire(cache_key, WINDOW + 5)

    return await call_next(request)
