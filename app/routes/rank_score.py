from datetime import datetime, timedelta, timezone
import redis.asyncio as redis

from fastapi import APIRouter, Depends, HTTPException

from dependencies.redis import get_redis


router = APIRouter()


KST = timezone(timedelta(hours=9))


def get_ranking_key() -> str:
    """
    KST 기준 리더보드
    """
    return f"leaderboard:daily:{datetime.now(KST).date()}"


def get_midnight_expiry() -> int:
    tomorrow = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        days=1
    )
    return int(tomorrow.timestamp())


@router.post("/rank/score")
async def update_score(user_id: int, score_delta: float, rd: redis.Redis = Depends(get_redis)):
    key = get_ranking_key()

    # ZINCRBY: 기존 점수에 합산
    new_score = await rd.zincrby(key, score_delta, user_id)
    await rd.expireat(key, get_midnight_expiry(), nx=True)
    return {"user_id": user_id, "current_score": new_score}


@router.get("/rank/top10")
async def get_top_rankers(rd: redis.Redis = Depends(get_redis)):
    key = get_ranking_key()
    top_rankers = await rd.zrange(key, 0, 9, desc=True, withscores=True)
    result = [{"rank": i + 1, "user_id": m, "score": s} for i, (m, s) in enumerate(top_rankers)]
    return {"top_rankers": result}


@router.get("/rank/{user_id}")
async def get_user_rank(user_id: int, rd: redis.Redis = Depends(get_redis)):
    key = get_ranking_key()

    async with rd.pipeline() as pipe:
        pipe.zrevrank(key, user_id)
        pipe.zscore(key, user_id)
        rank, score = await pipe.execute()

    if rank is None:
        raise HTTPException(status_code=404, detail="Ranking data not found")
    return {"user_id": user_id, "rank": rank + 1, "score": score}


@router.get("/rank/around/{user_id}")
async def get_nearby_rank(user_id: int, rd: redis.Redis = Depends(get_redis)):
    key = get_ranking_key()
    my_rank = await rd.zrevrank(key, user_id)

    if my_rank is None:
        raise HTTPException(status_code=404, detail="Ranking data not found")

    start = max(0, my_rank - 2)
    end = my_rank + 2

    nearby_list = await rd.zrange(key, start, end, desc=True, withscores=True)

    result = [
        {"rank": start + i + 1, "user_id": m, "score": s} for i, (m, s) in enumerate(nearby_list)
    ]
    return {"user_id": user_id, "nearby_rankers": result}
