"""
Redis 직접 연결 부하 테스트 (파이프라인 + 멀티스레드)
Usage: python traffic_redis_direct.py
"""

import redis
import time
import random
import threading

pool = redis.ConnectionPool(
    host="localhost", port=6379, db=0, decode_responses=True, max_connections=50
)
rd = redis.Redis(connection_pool=pool)

print("Redis 멀티스레드 트래픽 발생기 시작 (종료: Ctrl+C)")
print("Grafana 자동 새로고침을 5s로 설정하세요\n")

# 500KB 더미 데이터 — 메모리 사용량 변동 극대화
dummy_data = "A" * (1024 * 500)


def worker_thread(thread_id: int):
    try:
        while True:
            pipe = rd.pipeline()
            for _ in range(200):
                key_id = random.randint(1, 10000)
                cache_key = f"fake:data:{key_id}"

                pipe.get(cache_key)

                # 10% 확률로 500KB 쓰기 → 짧은 TTL로 메모리 급등/급락 유발
                if random.random() < 0.1:
                    pipe.set(cache_key, dummy_data, ex=random.randint(2, 5))

                pipe.incr("fake:global:hits")

            pipe.execute()
            time.sleep(0.01)
    except Exception as e:
        print(f"Thread {thread_id} stopped: {e}")


threads = []
for i in range(5):
    t = threading.Thread(target=worker_thread, args=(i,), daemon=True)
    t.start()
    threads.append(t)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n트래픽 발생 중단")
