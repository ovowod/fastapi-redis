from datetime import date

import redis.asyncio as redis
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies.db import get_db
from dependencies.redis import get_redis

router = APIRouter()


@router.post("/article/{article_id}/view")
async def increase_view_count(
    article_id: int,
    user_id: int,
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
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

    # Redis miss → DB에서 view_count warmup
    if not await rd.exists(view_count_key):
        result = await db.execute(
            text("SELECT view_count FROM articles WHERE id = :id"),
            {"id": article_id},
        )
        row = result.fetchone()
        if row:
            await rd.set(view_count_key, row.view_count)

    # write-back: Redis에만 기록하고 즉시 응답, 백그라운드에서 DB에 반영
    pipe = rd.pipeline()
    pipe.sadd(viewer_key, user_id)
    pipe.expire(viewer_key, 86400)
    pipe.incr(view_count_key)
    pipe.sadd("dirty:view_articles", article_id)  # flush 대상으로 등록
    result = await pipe.execute()

    return {
        "article_id": article_id,
        "total_view_count": result[2],
        "duplicated": False,
    }


@router.post("/article/{article_id}/like")
async def increase_like_count(
    article_id: int,
    user_id: int,
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    liked_key = f"user:{user_id}:liked_articles"
    like_count_key = f"article:{article_id}:like_count"

    # Redis miss → DB에서 liked_articles warmup
    if not await rd.exists(liked_key):
        result = await db.execute(
            text("SELECT article_id FROM article_likes WHERE user_id = :user_id"),
            {"user_id": user_id},
        )
        rows = result.fetchall()
        if rows:
            await rd.sadd(liked_key, *[row.article_id for row in rows])

    # Redis miss → DB에서 like_count warmup
    if not await rd.exists(like_count_key):
        result = await db.execute(
            text("SELECT like_count FROM articles WHERE id = :id"),
            {"id": article_id},
        )
        row = result.fetchone()
        if row:
            await rd.set(like_count_key, row.like_count)

    already_liked = await rd.sismember(liked_key, article_id)

    if already_liked:
        # 좋아요 취소: count는 write-back, 관계 데이터(article_likes)는 write-through
        pipe = rd.pipeline()
        pipe.srem(liked_key, article_id)
        pipe.decr(like_count_key)
        pipe.sadd("dirty:like_articles", article_id)  # flush 대상으로 등록
        result = await pipe.execute()

        # write-through: 누가 눌렀는지는 즉시 DB 반영
        await db.execute(
            text("DELETE FROM article_likes WHERE user_id = :user_id AND article_id = :article_id"),
            {"user_id": user_id, "article_id": article_id},
        )
        await db.commit()

        return {"article_id": article_id, "liked": False, "total_like_count": result[1]}

    # 좋아요 추가: count는 write-back, 관계 데이터(article_likes)는 write-through
    pipe = rd.pipeline()
    pipe.sadd(liked_key, article_id)
    pipe.incr(like_count_key)
    pipe.sadd("dirty:like_articles", article_id)  # flush 대상으로 등록
    result = await pipe.execute()

    # write-through: 누가 눌렀는지는 즉시 DB 반영
    await db.execute(
        text(
            "INSERT IGNORE INTO article_likes (user_id, article_id) VALUES (:user_id, :article_id)"
        ),
        {"user_id": user_id, "article_id": article_id},
    )
    await db.commit()

    return {"article_id": article_id, "liked": True, "total_like_count": result[1]}


@router.get("/article/{article_id}/stats")
async def get_article_stats(
    article_id: int,
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    view_count_key = f"article:{article_id}:view_count"
    like_count_key = f"article:{article_id}:like_count"

    view_count, like_count = await rd.mget(view_count_key, like_count_key)

    # Redis miss → DB fallback + warmup
    if view_count is None or like_count is None:
        result = await db.execute(
            text("SELECT view_count, like_count FROM articles WHERE id = :id"),
            {"id": article_id},
        )
        row = result.fetchone()
        if row:
            if view_count is None:
                await rd.set(view_count_key, row.view_count)
                view_count = row.view_count
            if like_count is None:
                await rd.set(like_count_key, row.like_count)
                like_count = row.like_count

    return {
        "article_id": article_id,
        "view_count": int(view_count) if view_count else 0,
        "like_count": int(like_count) if like_count else 0,
    }
