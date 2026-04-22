import uuid
import time
import asyncio

from fastapi import APIRouter, HTTPException, Depends
import redis.asyncio as redis
from redis.asyncio.lock import Lock
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from dependencies.redis import get_redis
from dependencies.db import get_db
from db.models import Stock


router = APIRouter()


_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


async def acquire_lock(
    rd, lock_name: str, acquire_timeout: float = 10.0, lock_timeout_ms: int = 5000
):
    identifier = str(uuid.uuid4())

    end_time = time.time() + acquire_timeout

    while time.time() < end_time:
        if await rd.set(lock_name, identifier, nx=True, px=lock_timeout_ms):
            return identifier

        await asyncio.sleep(0.1)

    return False


async def release_lock(rd, lock_name: str, identifier: str):
    release = rd.register_script(_RELEASE_SCRIPT)
    result = await release(keys=[lock_name], args=[identifier])
    return bool(result)


async def enqueue(rd, queue_name: str, request_id: str):
    await rd.rpush(queue_name, request_id)


async def wait_for_turn(rd, queue_name: str, request_id: str, timeout: float = 30.0) -> bool:
    # handoff 대신 폴링 방식 사용: 타임아웃 시 LREM으로 스스로 제거하므로
    # 살아있는 요청만 맨 앞에 남아 고아 lock 문제가 자연스럽게 해소
    end_time = time.time() + timeout
    while time.time() < end_time:
        front = await rd.lindex(queue_name, 0)
        if front == request_id:
            return True
        await asyncio.sleep(0.1)
    await rd.lrem(queue_name, 1, request_id)
    return False


async def dequeue(rd, queue_name: str):
    await rd.lpop(queue_name)


@router.post("/stock/reduce/{item_id}")
async def reduce_stock(
    item_id: str,
    user_id: str = "unknown",
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    lock_name = f"lock:item:{item_id}"

    lock_id = await acquire_lock(rd, lock_name)
    if not lock_id:
        raise HTTPException(
            status_code=409,
            detail=f"Too many requests. Please try again later. (user: {user_id})",
        )

    try:
        result = await db.execute(select(Stock).where(Stock.item_id == item_id))
        stock = result.scalar_one_or_none()

        if stock is None:
            raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

        if stock.quantity <= 0:
            raise HTTPException(status_code=409, detail=f"Item {item_id} is out of stock")

        stock.quantity -= 1
        await db.commit()

        print(
            f"[Lock acquired] Stock reduced for item {item_id} (user: {user_id}, remaining: {stock.quantity})"
        )
        return {
            "message": f"Item {item_id} stock reduced successfully",
            "user": user_id,
            "remaining": stock.quantity,
        }

    finally:
        await release_lock(rd, lock_name, lock_id)


@router.post("/stock/queue-reduce/{item_id}")
async def queue_reduce_stock(
    item_id: str,
    user_id: str = "unknown",
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    queue_name = f"queue:item:{item_id}"
    lock_name = f"lock:item:{item_id}"
    request_id = str(uuid.uuid4())

    # 큐 뒤에 줄 서기. 먼저 온 순서대로 처리됨 (FIFO)
    await enqueue(rd, queue_name, request_id)

    # 맨 앞 자리가 될 때까지 대기.
    # 타임아웃 시 LREM으로 스스로 큐에서 제거하고 포기
    my_turn = await wait_for_turn(rd, queue_name, request_id)
    if not my_turn:
        raise HTTPException(
            status_code=409,
            detail=f"Queue wait timed out. Please try again later. (user: {user_id})",
        )

    # 내 차례가 됐을 때 lock 추가로 획득
    lock_id = await acquire_lock(rd, lock_name, acquire_timeout=5.0)
    if not lock_id:
        await dequeue(rd, queue_name)
        raise HTTPException(
            status_code=409,
            detail=f"Failed to acquire lock. Please try again later. (user: {user_id})",
        )

    try:
        result = await db.execute(select(Stock).where(Stock.item_id == item_id))
        stock = result.scalar_one_or_none()

        if stock is None:
            raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

        if stock.quantity <= 0:
            raise HTTPException(status_code=409, detail=f"Item {item_id} is out of stock")

        stock.quantity -= 1
        await db.commit()

        print(
            f"[Queue] Stock reduced for item {item_id} (user: {user_id}, remaining: {stock.quantity})"
        )
        return {
            "message": f"Item {item_id} stock reduced successfully",
            "user": user_id,
            "remaining": stock.quantity,
        }

    finally:
        await release_lock(rd, lock_name, lock_id)
        # 처리 완료 후 큐에서 제거
        await dequeue(rd, queue_name)


@router.post("/stock/redlock-reduce/{item_id}")
async def redlock_reduce_stock(
    item_id: str,
    user_id: str = "unknown",
    rd: redis.Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    lock_name = f"lock:item:{item_id}"

    try:
        async with Lock(
            rd,
            lock_name,
            timeout=5,  # 락 TTL(초): 크래시 나도 5초 후 자동 해제 → lock_timeout_ms=5000
            sleep=0.1,  # 락 획득 재시도 간격 → asyncio.sleep(0.1)
            blocking=True,  # True: blocking_timeout까지 대기, False: 즉시 예외
            blocking_timeout=10,  # 락 획득 최대 대기 시간(초) → acquire_timeout=10.0
        ):
            result = await db.execute(select(Stock).where(Stock.item_id == item_id))
            stock = result.scalar_one_or_none()

            if stock is None:
                raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

            if stock.quantity <= 0:
                raise HTTPException(status_code=409, detail=f"Item {item_id} is out of stock")

            stock.quantity -= 1
            await db.commit()

            print(
                f"[Redlock] Stock reduced for item {item_id} (user: {user_id}, remaining: {stock.quantity})"
            )
            return {
                "message": f"Item {item_id} stock reduced successfully",
                "user": user_id,
                "remaining": stock.quantity,
            }
    except Exception as e:
        if "LockNotOwnedError" in type(e).__name__ or "LockError" in type(e).__name__:
            raise HTTPException(
                status_code=409,
                detail=f"Failed to acquire lock. Please try again. (user: {user_id})",
            )
        raise


@router.post("/stock/reset/{item_id}")
async def reset_stock(
    item_id: str,
    quantity: int = 5,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Stock).where(Stock.item_id == item_id))
    stock = result.scalar_one_or_none()

    if stock is None:
        stock = Stock(item_id=item_id, quantity=quantity)
        db.add(stock)
    else:
        stock.quantity = quantity

    await db.commit()
    return {"item_id": item_id, "quantity": stock.quantity}
