import uuid
from datetime import datetime, timezone
import redis.asyncio as redis

from pydantic import BaseModel

from fastapi import APIRouter, Depends, Request, Response, Cookie, HTTPException

from dependencies.redis import get_redis


SESSION_EXPIRE = 3600


class LoginRequest(BaseModel):
    user_id: int


class UserInfo(BaseModel):
    user_id: int
    ip: str
    last_login: datetime


router = APIRouter()


@router.post("/login")
async def login(
    req: LoginRequest,
    response: Response,
    request: Request,
    rd: redis.Redis = Depends(get_redis),
):
    session_id = str(uuid.uuid4())
    session_key = f"session:{session_id}"

    user_info = UserInfo(
        user_id=req.user_id,
        ip=request.client.host if request.client else "127.0.0.1",
        last_login=datetime.now(timezone.utc),
    )
    await rd.set(session_key, user_info.model_dump_json(), ex=SESSION_EXPIRE)

    response.set_cookie(
        key="session_id", value=session_id, httponly=True, secure=False, samesite="lax"
    )
    return {"message": "Login successful", "session_id": session_id}


@router.get("/me")
async def get_me(session_id: str = Cookie(default=None), rd: redis.Redis = Depends(get_redis)):
    if not session_id:
        raise HTTPException(status_code=401, detail="No session")

    session_key = f"session:{session_id}"
    data = await rd.get(session_key)

    if not data:
        raise HTTPException(status_code=401, detail="Session expired")

    return UserInfo.model_validate_json(data)
