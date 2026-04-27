import uuid

from fastapi import Request
from fastapi.responses import JSONResponse


LIMIT = 5
WINDOW = 60


LUA_SCRIPT = """
local key = KEYS[1]
local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1e6
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local member = ARGV[3]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)

if count >= limit then
    return 0
end

redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, window + 5)
return 1
"""


async def rate_limit_sliding_window(request: Request, call_next):
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis
    user_identifier = request.client.host if request.client else "127.0.0.1"
    cache_key = f"rate_limit:{user_identifier}"
    member = uuid.uuid4().hex[:6]

    result = await rd.eval(LUA_SCRIPT, 1, cache_key, WINDOW, LIMIT, member)

    if result == 0:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "detail": f"Rate limit exceeded",
            },
        )

    return await call_next(request)
