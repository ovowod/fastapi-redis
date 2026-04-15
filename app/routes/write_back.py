import redis.asyncio as redis

from fastapi import APIRouter, Depends

from dependencies.redis import get_redis


router = APIRouter()


@router.post("/article/{article_id}/view")
async def increase_view_count(article_id: int, rd: redis.Redis = Depends(get_redis)):
    view_key = f"article:{article_id}:view_count"

    current_view_count = await rd.incr(view_key)

    # TODO: DB Write-Back
    if current_view_count:
        ...

    return {"article_id": article_id, "total_view_count": current_view_count}


@router.post("/article/{article_id}/like")
async def increase_like_count(article_id: int, rd: redis.Redis = Depends(get_redis)):
    like_key = f"article:{article_id}:like_count"

    current_like_count = await rd.incr(like_key)

    # TODO: DB Write-Back
    if current_like_count:
        ...

    return {"article_id": article_id, "total_like_count": current_like_count}


@router.get("/article/{article_id}/stats")
async def get_article_stats(article_id: int, rd: redis.Redis = Depends(get_redis)):
    view_key = f"article:{article_id}:view_count"
    like_key = f"article:{article_id}:like_count"

    # view_count = await rd.get(view_key)
    # like_count = await rd.get(like_key)

    view_count, like_count = await rd.mget(view_key, like_key)

    return {
        "article_id": article_id,
        "view_count": int(view_count) if view_count else 0,
        "like_count": int(like_count) if like_count else 0,
    }
