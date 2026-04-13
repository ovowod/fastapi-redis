import asyncio
import json
import redis.asyncio as redis

from fastapi import APIRouter, Depends, HTTPException

from dependencies.redis import get_redis
from dependencies.db import get_db

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db.models import User


router = APIRouter()


@router.get("/user/{user_id}")
async def get_user_profile(user_id: int, rd: redis.Redis = Depends(get_redis), db: AsyncSession = Depends(get_db)):
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

    user_data = {"id": user.id, "name": user.name, "email": user.email}
    await rd.set(cache_key, json.dumps(user_data), ex=300)
    return user_data
