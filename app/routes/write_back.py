from datetime import date
import redis.asyncio as redis

from fastapi import APIRouter, Depends

from dependencies.redis import get_redis


router = APIRouter()


@router.post("/article/{article_id}/view")
async def increase_view_count(article_id: int, user_id: int, rd: redis.Redis = Depends(get_redis)):
    today = date.today().isoformat()
    viewer_key = f"article:{article_id}:viewer:{today}"
    view_count_key = f"article:{article_id}:view_count"

    already_viewer = await rd.sismember(viewer_key, user_id)
    if already_viewer:
        view_count = await rd.get(view_count_key)
        return {
            "article_id": article_id,
            "total_view_count": int(view_count) or 0,
            "duplicated": True,
        }

    pipe = rd.pipeline()
    pipe.sadd(viewer_key, user_id)
    pipe.expire(viewer_key, 86400)
    pipe.incr(view_count_key)
    result = await pipe.execute()

    # TODO: DB Write-Back

    return {
        "article_id": article_id,
        "total_view_count": result[2],
        "duplicated": False,
    }


@router.post("/article/{article_id}/like")
async def increase_like_count(article_id: int, user_id: int, rd: redis.Redis = Depends(get_redis)):
    liked_key = f"user:{user_id}:liked_articles"
    like_count_key = f"article:{article_id}:like_count"

    already_liked = await rd.sismember(liked_key, article_id)
    if already_liked:
        pipe = rd.pipeline()
        pipe.srem(liked_key, article_id)
        pipe.decr(like_count_key)
        result = await pipe.execute()

        return {"article_id": article_id, "liked": False, "total_like_count": result[1]}

    pipe = rd.pipeline()
    pipe.sadd(liked_key, article_id)
    pipe.incr(like_count_key)
    result = await pipe.execute()

    # TODO: DB Write-Back

    return {"article_id": article_id, "liked": True, "total_like_count": result[1]}


@router.get("/article/{article_id}/stats")
async def get_article_stats(article_id: int, rd: redis.Redis = Depends(get_redis)):
    view_count_key = f"article:{article_id}:view_count"
    like_count_key = f"article:{article_id}:like_count"

    # view_count = await rd.get(view_key)
    # like_count = await rd.get(like_key)

    view_count, like_count = await rd.mget(view_count_key, like_count_key)

    return {
        "article_id": article_id,
        "view_count": int(view_count) if view_count else 0,
        "like_count": int(like_count) if like_count else 0,
    }
