from fastapi import Request
from fastapi.responses import JSONResponse

CAPACITY = 10
REFILL_RATE = 0.1
TTL = 65


LUA_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1e6

local stored = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens

if stored[1] == false then
    tokens = capacity
else
    local elapsed = now - tonumber(stored[2])
    tokens = math.min(capacity, tonumber(stored[1]) + elapsed * refill_rate)
end

if tokens < 1 then
    return 0
end

redis.call('HSET', key, 'tokens', tokens - 1, 'last_refill', now)
redis.call('EXPIRE', key, ttl)
return 1
"""


async def rate_limit_token_bucket(request: Request, call_next):
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis
    user_identifier = request.client.host if request.client else "127.0.0.1"
    cache_key = f"rate_limit:{user_identifier}"

    result = await rd.eval(LUA_SCRIPT, 1, cache_key, CAPACITY, REFILL_RATE, TTL)

    if result == 0:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "detail": f"Rate limit exceeded",
            },
        )

    return await call_next(request)
