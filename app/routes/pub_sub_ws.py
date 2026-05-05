import asyncio

import redis.asyncio as redis

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from dependencies.redis import get_redis

router = APIRouter()

NOTICE_CHANNEL = "system:notices:ws"
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


@router.get("/pub_sub_ws")
async def ws_index():
    return FileResponse("templates/pub_sub_ws.html")


@router.websocket("/ws/notices")
async def ws_stream_notices(websocket: WebSocket, rd: redis.Redis = Depends(get_redis)):
    await websocket.accept()

    subscribers: set[asyncio.Queue] = websocket.app.state.ws_subscribers
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    subscribers.add(queue)

    async def send_loop():
        while True:
            data = await queue.get()
            if data is None:  # drop-oldest 종료 신호
                await websocket.close()
                return
            await websocket.send_text(data)

    send_task = asyncio.create_task(send_loop())
    try:
        while True:
            text = await websocket.receive_text()  # 클라이언트 메시지 수신 & 끊김 감지
            await rd.publish(NOTICE_CHANNEL, text)  # 받은 즉시 Redis에 publish
    except WebSocketDisconnect:
        pass
    finally:
        send_task.cancel()
        subscribers.discard(queue)
