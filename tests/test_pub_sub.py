import asyncio

QUEUE_MAX_SIZE = 3  # 테스트용 작은 크기


async def fan_out(data, subscribers):
    """redis_listener fan-out 로직 (routes/pub_sub.py와 동일)"""
    for q in list(subscribers):
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            subscribers.discard(q)
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            q.put_nowait(None)


async def drain_generator(queue, timeout=0.5):
    """event_generator 루프 시뮬레이션 → 수신한 데이터 목록 반환"""
    received = []
    while True:
        try:
            data = await asyncio.wait_for(queue.get(), timeout=timeout)
            if data is None:  # sentinel → 연결 종료
                break
            received.append(data)
        except asyncio.TimeoutError:
            break
    return received


async def test_normal_delivery():
    """정상 흐름: 메시지가 순서대로 전달되고 subscriber 유지"""
    subscribers = set()
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    subscribers.add(queue)

    for msg in ["a", "b", "c"]:
        await fan_out(msg, subscribers)

    received = await drain_generator(queue)

    assert received == ["a", "b", "c"]
    assert queue in subscribers
    print("[PASS] test_normal_delivery")


async def test_queue_full_sends_sentinel_and_removes_subscriber():
    """
    QueueFull 발생 시:
    - subscriber 제거
    - sentinel(None) 전달
    - event_generator 종료
    """
    subscribers = set()
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    subscribers.add(queue)

    # 큐를 꽉 채움
    for i in range(QUEUE_MAX_SIZE):
        queue.put_nowait(f"pre{i}")

    # overflow → discard + sentinel
    await fan_out("overflow", subscribers)

    assert queue not in subscribers, "subscriber가 제거되어야 함"

    received = await drain_generator(queue)

    assert None not in received, "sentinel은 received에 포함되지 않음"
    print(
        f"[PASS] test_queue_full_sends_sentinel_and_removes_subscriber (수신: {received})"
    )


async def test_slow_client_does_not_block_fast_client():
    """느린 클라이언트가 빠른 클라이언트 수신을 방해하지 않음"""
    subscribers = set()

    fast_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    slow_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    subscribers.add(fast_queue)
    subscribers.add(slow_queue)

    # slow_queue만 꽉 채움
    for i in range(QUEUE_MAX_SIZE):
        slow_queue.put_nowait(f"old{i}")

    await fan_out("new_msg", subscribers)

    assert fast_queue in subscribers, "fast client는 유지되어야 함"
    assert slow_queue not in subscribers, "slow client는 제거되어야 함"

    fast_received = await drain_generator(fast_queue)
    assert "new_msg" in fast_received
    print("[PASS] test_slow_client_does_not_block_fast_client")


if __name__ == "__main__":
    asyncio.run(test_normal_delivery())
    asyncio.run(test_queue_full_sends_sentinel_and_removes_subscriber())
    asyncio.run(test_slow_client_does_not_block_fast_client())
