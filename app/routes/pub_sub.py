import asyncio
import redis.asyncio as redis

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse

from sse_starlette.sse import EventSourceResponse

from dependencies.redis import get_redis

router = APIRouter()

NOTICE_CHANNEL = "system:notices"


# 연결된 클라이언트 큐 목록
subscribers: set[asyncio.Queue] = set()


# 앱 시작 시 백그라운드에서 Redis 구독
async def redis_listener(rd):
    while True:
        try:
            async with rd.pubsub() as pubsub:
                await pubsub.subscribe(NOTICE_CHANNEL)
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

                    if message:
                        for q in list(subscribers):  # 순회 중 동시 수정 방지
                            await q.put(message["data"])
                    await asyncio.sleep(0.1)
        except Exception as e:
            print(f"redis_listener error: {e}, retrying in 3s...")
            await asyncio.sleep(3)


@router.get("/pub_sub")
async def index():
    return FileResponse("templates/pub_sub.html")


@router.post("/publish-notice")
async def send_notice(message: str, rd: redis.Redis = Depends(get_redis)):
    """
    publish
    """
    subscriber_count = await rd.publish(NOTICE_CHANNEL, message)
    return {
        "message": message,
        "status": "success",
        "received_subscribers": subscriber_count,
    }


@router.get("/stream-notices")
async def sse_stream_notices(request: Request):
    """
    subscribe: SSE 방식
    """

    queue = asyncio.Queue()
    subscribers.add(queue)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield {"data": data}
                except asyncio.TimeoutError:
                    continue
        finally:
            subscribers.discard(queue)

    return EventSourceResponse(event_generator())
