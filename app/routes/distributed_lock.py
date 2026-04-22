import uuid
import time
import asyncio

from fastapi import APIRouter, HTTPException, Depends
import redis.asyncio as redis
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
