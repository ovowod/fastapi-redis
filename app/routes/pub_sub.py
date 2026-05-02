import asyncio
from datetime import datetime, timedelta, timezone
import redis.asyncio as redis

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse

from sse_starlette.sse import EventSourceResponse

from dependencies.redis import get_redis

router = APIRouter()

NOTICE_CHANNEL = "system:notices"


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
async def sse_stream_notices(request: Request, rd: redis.Redis = Depends(get_redis)):
    """
    subscribe: SSE 방식
    """

    async def event_generator():
        # rd.pubsub 으로 구독 전용 객체 생성
        async with rd.pubsub() as pubsub:
            await pubsub.subscribe(NOTICE_CHANNEL)

            try:
                while True:
                    if await request.is_disconnected():
                        break
                    # 메시지 수신 대기
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message:
                        print("message:", message)
                        yield {"data": message["data"]}
                    await asyncio.sleep(0.1)
            finally:
                await pubsub.unsubscribe(NOTICE_CHANNEL)

    return EventSourceResponse(event_generator())
