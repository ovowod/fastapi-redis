import random
import hashlib

from fastapi import HTTPException, APIRouter, Depends
from pydantic import BaseModel
import redis.asyncio as redis

from dependencies.redis import get_redis


router = APIRouter()


AUTH_TIMEOUT = 300


class SendCodeRequest(BaseModel):
    phone: str


class VerifyCodeRequest(BaseModel):
    phone: str
    input_code: str


@router.post("/auth/send")
async def send_verification_code(req_data: SendCodeRequest, rd: redis.Redis = Depends(get_redis)):
    code = str(random.randint(100000, 999999))

    hashed_phone = hashlib.sha256(req_data.phone.encode()).hexdigest()
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

    verify_code = await rd.get(cache_key)

    if not verify_code:
        raise HTTPException(status_code=400, detail="Code expired or not requested")

    if verify_code != req_data.input_code:
        raise HTTPException(status_code=400, detail="Invalid code")

    await rd.delete(cache_key)

    return {"message": "Authentication successful"}
