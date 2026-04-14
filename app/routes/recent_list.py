import redis.asyncio as redis

from fastapi import APIRouter, Depends

from dependencies.redis import get_redis


router = APIRouter()


@router.post("/product/{product_id}/view")
async def view_product(product_id: int, rd: redis.Redis = Depends(get_redis), user_id: int = 1):
    key = f"user:{user_id}:recent_views"

    await rd.lrem(key, 0, str(product_id))
    await rd.lpush(key, str(product_id))
    await rd.ltrim(key, 0, 4)

    return {"message": f"Product {product_id} added to recent views."}


@router.get("/user/{user_id}/recent-views")
async def get_recent_views(user_id: int, rd: redis.Redis = Depends(get_redis)):
    key = f"user:{user_id}:recent_views"

    views = await rd.lrange(key, 0, -1)
    return {"recent_views": views}
