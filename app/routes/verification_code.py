import random
import hashlib

from fastapi import HTTPException, APIRouter, Depends
from pydantic import BaseModel
import redis.asyncio as redis

from dependencies.redis import get_redis


router = APIRouter()


AUTH_TIMEOUT = 300

VERIFY_AND_DELETE_SCRIPT = """
local stored = redis.call("GET", KEYS[1])
if not stored then
    return 0
end
if stored == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1
end
return -1
"""


class SendCodeRequest(BaseModel):
    phone: str


class VerifyCodeRequest(BaseModel):
    phone: str
    input_code: str


@router.post("/auth/send")
async def send_verification_code(req_data: SendCodeRequest, rd: redis.Redis = Depends(get_redis)):
    code = str(random.randint(100000, 999999))

    hashed_phone = hashlib.sha256(req_data.phone.encode()).hexdigest()
    is_allowed = await rd.set(f"auth:limit:{hashed_phone}", "limit", ex=60, nx=True)

    if not is_allowed:
        limit_time = await rd.ttl(f"auth:limit:{hashed_phone}")
        raise HTTPException(
            status_code=429,
            detail=f"Please wait {int(limit_time)} seconds before requesting again",
        )

    cache_key = f"auth:code:{hashed_phone}"

    await rd.set(cache_key, code, ex=AUTH_TIMEOUT)

    print(f"SMS TO: {req_data.phone}, CODE: {code}")

    return {
        "message": "Verification code sent",
        "code": code,
        "expires_in": AUTH_TIMEOUT,
    }


@router.post("/auth/verify")
async def verify_code(req_data: VerifyCodeRequest, rd: redis.Redis = Depends(get_redis)):
    hashed_phone = hashlib.sha256(req_data.phone.encode()).hexdigest()
    cache_key = f"auth:code:{hashed_phone}"

    verify_and_delete = rd.register_script(VERIFY_AND_DELETE_SCRIPT)
    result = await verify_and_delete(keys=[cache_key], args=[req_data.input_code])

    if result == 0:
        raise HTTPException(status_code=400, detail="Code expired or not requested")

    if result == -1:
        raise HTTPException(status_code=400, detail="Invalid code")

    return {"message": "Authentication successful"}
