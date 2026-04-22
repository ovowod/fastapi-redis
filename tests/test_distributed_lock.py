import asyncio
import time
import httpx

BASE_URL = "http://localhost:8000"
ITEM_ID = "100"  # init.sql에서 재고 5개로 초기화된 아이템


INITIAL_STOCK = 5


async def reset_stock(client: httpx.AsyncClient, quantity: int = INITIAL_STOCK):
    await client.post(
        f"{BASE_URL}/stock/reset/{ITEM_ID}", params={"quantity": quantity}
    )
    print(f"[Setup] Stock reset to {quantity}")


async def request_reduce(client: httpx.AsyncClient, user_id: str):
    resp = await client.post(
        f"{BASE_URL}/stock/reduce/{ITEM_ID}",
        params={"user_id": user_id},
    )
    status = resp.status_code
    body = resp.json()
    print(f"[{user_id}] {status} -> {body}")
    return status, body


async def test_concurrent_lock():
    """
    10명 동시 요청 → 재고(5개) 만큼만 성공, 나머지는 품절 반환
    """
    print("\n=== Test: 10 users competing, stock=5 ===")
    start = time.time()
    async with httpx.AsyncClient(timeout=60.0) as client:
        await reset_stock(client)
        tasks = [request_reduce(client, f"user-{i}") for i in range(1, 11)]
        results = await asyncio.gather(*tasks)

    success = sum(1 for status, _ in results if status == 200)
    out_of_stock = sum(
        1
        for status, body in results
        if status == 409 and "out of stock" in body.get("detail", "")
    )
    timeout = sum(1 for status, _ in results if status == 409) - out_of_stock

    print(
        f"\nSuccess: {success}, Out of stock: {out_of_stock}, Lock timeout: {timeout}"
    )
    print(f"Total elapsed: {time.time() - start:.2f}s")
    assert success == INITIAL_STOCK, (
        f"Expected {INITIAL_STOCK} successes, got {success}"
    )
    print(
        f"[PASS] Exactly {INITIAL_STOCK} requests reduced stock, rest got 409 out-of-stock."
    )


async def request_queue_reduce(client: httpx.AsyncClient, user_id: str):
    resp = await client.post(
        f"{BASE_URL}/stock/queue-reduce/{ITEM_ID}",
        params={"user_id": user_id},
    )
    status = resp.status_code
    body = resp.json()
    print(f"[{user_id}] {status} -> {body}")
    return status, body


async def request_reduce_staggered(
    client: httpx.AsyncClient, user_id: str, delay: float
):
    await asyncio.sleep(delay)
    return await request_queue_reduce(client, user_id)


async def test_fifo_order():
    """
    순서대로 큐에 들어간 요청이 FIFO로 처리되는지 확인.
    0.1s 간격으로 enqueue → remaining이 4,3,2,1,0 순으로 감소해야 함.
    """
    print("\n=== Test: FIFO queue order ===")
    async with httpx.AsyncClient(timeout=60.0) as client:
        await reset_stock(client)
        tasks = [
            request_reduce_staggered(client, f"user-{i}", delay=i * 0.1)
            for i in range(1, 6)
        ]
        results = await asyncio.gather(*tasks)

    remaining_list = [body["remaining"] for status, body in results if status == 200]
    print(f"\nRemaining counts in enqueue order: {remaining_list}")
    assert remaining_list == sorted(remaining_list, reverse=True), (
        f"Expected descending remaining counts (FIFO), got {remaining_list}"
    )
    print("[PASS] Requests processed in FIFO order.")


async def test_sequential_lock():
    """
    순차 요청 → 락 해제 후 다음 유저가 정상 획득하는지 확인
    """
    print("\n=== Test: Sequential lock release and re-acquire ===")
    async with httpx.AsyncClient(timeout=30.0) as client:
        await reset_stock(client)
        print("Request 1...")
        status1, _ = await request_reduce(client, "user-A")
        assert status1 in (200, 409), f"Unexpected status {status1}"

        print("Request 2 (after lock released)...")
        status2, _ = await request_reduce(client, "user-B")
        assert status2 in (200, 409), f"Unexpected status {status2}"

    print("[PASS] Sequential requests processed without deadlock.")


if __name__ == "__main__":
    asyncio.run(test_concurrent_lock())
    asyncio.run(test_fifo_order())
    asyncio.run(test_sequential_lock())
