import asyncio
import redis.asyncio as redis

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse

from sse_starlette.sse import EventSourceResponse

from dependencies.redis import get_redis

router = APIRouter()

NOTICE_CHANNEL = "system:notices"
QUEUE_MAX_SIZE = 100


async def redis_listener(rd, subscribers: set[asyncio.Queue]):
    """
    앱 전체에서 Redis 구독을 하나만 유지하고, 메시지를 각 클라이언트 큐에 fan-out
    """
    while True:
        try:
            async with rd.pubsub() as pubsub:
                await pubsub.subscribe(NOTICE_CHANNEL)
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message:
                        for q in list(subscribers):  # 순회 중 set 변경 방지용 복사
                            try:
                                q.put_nowait(message["data"])
                            except asyncio.QueueFull:
                                # 큐가 가득 찬 클라이언트는 drop-oldest 후 종료 신호 전송
                                subscribers.discard(q)
                                try:
                                    q.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                q.put_nowait(None)  # event_generator 종료 트리거
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
    Redis 채널에 메시지를 publish
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
    SSE로 공지를 구독
    """
    subscribers: set[asyncio.Queue] = request.app.state.subscribers

    # 연결마다 독립 큐를 생성하고 공유 set에 등록 → redis_listener가 fan-out
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    subscribers.add(queue)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    # timeout으로 disconnect 체크를 주기적으로 수행
                    data = await asyncio.wait_for(queue.get(), timeout=1.0)
                    if data is None:
                        break
                    yield {"data": data}
                except asyncio.TimeoutError:
                    continue
        finally:
            subscribers.discard(queue)

    return EventSourceResponse(event_generator())
