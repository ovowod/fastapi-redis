"""
Write-back 패턴 구현

요청이 들어오면 Redis에만 count를 기록하고 즉시 응답한 뒤,
백그라운드에서 주기적으로 DB에 반영한다.

count는 데이터 정합성을 강하게 요구하지 않으므로
약간의 지연(최대 FLUSH_INTERVAL)을 허용한다.

dirty set 전략:
  - count가 변경된 article_id를 dirty:*_articles Set에 등록
  - flush 시점에 dirty set을 읽어 변경된 것만 DB에 반영
  - flush 전에 dirty set을 먼저 제거하여, flush 중 새로 들어온 write는
    다음 사이클에서 처리되도록 보장
"""

import asyncio

import redis.asyncio as redis
from sqlalchemy import text

from db.session import SessionLocal

FLUSH_INTERVAL = 60  # seconds


async def flush_view_counts(rd: redis.Redis):
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        try:
            dirty_ids = await rd.smembers("dirty:view_articles")
            if not dirty_ids:
                continue

            # flush 대상을 먼저 제거 — flush 중 새 write는 다시 추가되어 다음 사이클에 처리됨
            await rd.srem("dirty:view_articles", *dirty_ids)

            async with SessionLocal() as db:
                for article_id in dirty_ids:
                    view_count = await rd.get(f"article:{article_id}:view_count")
                    if view_count is not None:
                        await db.execute(
                            text("UPDATE articles SET view_count = :vc WHERE id = :id"),
                            {"vc": int(view_count), "id": int(article_id)},
                        )
                await db.commit()
                print(f"[write-back] view_count flushed: {len(dirty_ids)} articles")

        except Exception as e:
            print(f"[write-back] view flush error: {e}")


async def flush_like_counts(rd: redis.Redis):
    # like_count는 write-through로 관리되는 article_likes 테이블과 별개로
    # Redis의 INCR/DECR 값을 주기적으로 DB에 반영한다.
    # 카운트 오차는 허용하며, 정확도가 필요하다면 article_likes에서 COUNT(*)로 보정할 수 있다.
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        try:
            dirty_ids = await rd.smembers("dirty:like_articles")
            if not dirty_ids:
                continue

            await rd.srem("dirty:like_articles", *dirty_ids)

            async with SessionLocal() as db:
                for article_id in dirty_ids:
                    like_count = await rd.get(f"article:{article_id}:like_count")
                    if like_count is not None:
                        await db.execute(
                            text("UPDATE articles SET like_count = :lc WHERE id = :id"),
                            {"lc": int(like_count), "id": int(article_id)},
                        )
                await db.commit()
                print(f"[write-back] like_count flushed: {len(dirty_ids)} articles")

        except Exception as e:
            print(f"[write-back] like flush error: {e}")
