import asyncio
import json
import redis.asyncio as redis

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict

from dependencies.redis import get_redis
from dependencies.db import get_db

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db.models import User


class UserProfileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    email: str


class UserUpdateRequest(BaseModel):
    name: str
    email: str


router = APIRouter()


@router.get("/user/{user_id}", response_model=UserProfileResponse)
async def get_user_profile(
    user_id: int,
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"user:profile:{user_id}"

    cached_user = await rd.get(cache_key)

    if cached_user:
        print(f"Cache Hit! user_id: {user_id}")
        return json.loads(cached_user)

    print(f"Cache Miss.. Fetching from DB.")

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    await asyncio.sleep(2)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user_data = UserProfileResponse.model_validate(user)
    await rd.set(cache_key, user_data.model_dump_json(), ex=300)
    return user_data


@router.put("/user/{user_id}")
async def update_user_profile(
    user_id: int,
    profile: UserUpdateRequest,
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"user:profile:{user_id}"

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.name = profile.name
    user.email = profile.email

    await db.commit()
    await rd.delete(cache_key)

    await db.refresh(user)

    return {"id": user.id, "name": user.name, "email": user.email}
