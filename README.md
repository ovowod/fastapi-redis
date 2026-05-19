# FastAPI + Redis

Redis를 FastAPI에 붙여보며 각 사용 패턴과 트레이드오프를 정리한 학습 레포.

## 목차
1. [Cache-Aside](#cache-aside)
2. [Recent List](#recent-list)
3. [Session Store](#session-store)
4. [Article Stats](#article-stats)
5. [Verification Code](#verification-code)
6. [Distributed Lock](#distributed-lock)
7. [Rate Limiting](#rate-limiting)
8. [Leaderboard](#leaderboard)
9. [Pub/Sub](#pubsub)

---

## Cache-Aside

[app/routes/cache_aside.py](app/routes/cache_aside.py)

DB 조회나 외부 API 호출처럼 응답이 느린 요청은 Redis에 캐싱한다.
최초 요청 시에는 원본 소스에서 데이터를 가져오고, 이후 요청부터는 Redis에서 바로 반환한다.
데이터가 변경될 수 있으므로 영구 저장 대신 TTL을 설정한다.

```
GET /user/{id}
  └─ Redis hit  → return
  └─ Redis miss → MySQL → SET key EX 300 → return
```

```python
value = await redis.get(key)
if not value:
    value = await db.get(...)
    await redis.setex(key, 300, value)
```

데이터가 업데이트되면 캐시를 어떻게 처리할지 고민이 생긴다.

| 전략 | 방식 |
|------|------|
| write-through | 업데이트 시 캐시도 함께 갱신 → 항상 최신 유지 |
| delete-on-write | 업데이트 시 캐시 키 삭제 → 다음 읽기 때 재캐싱 |

Cache-Aside는 읽기 중심 패턴이므로, 아무도 읽지 않는 데이터까지 캐시에 올리는 write-through보다 실제로 읽힐 때만 캐싱되는 delete-on-write가 더 자연스럽다.

```
PUT /user/{id}
  └─ MySQL UPDATE → DEL key
```

```python
await redis.delete(key)
```

---

## Recent List

[app/routes/recent_list.py](app/routes/recent_list.py)

최근 본 상품처럼 유저마다 다르고 순서가 중요한 데이터를 관리한다.
유저별로 다른 목록을 DB에 저장하는 건 스키마 설계부터 중복 처리, 오래된 기록 정리까지 비용이 크다.

```
LREM   user:{user_id}:recent_views 0 {product_id}   ← 같은 항목이 있으면 먼저 제거
LPUSH  user:{user_id}:recent_views {product_id}     ← 맨 앞에 추가
LTRIM  user:{user_id}:recent_views 0 4              ← 최근 5개만 유지
LRANGE user:{user_id}:recent_views 0 -1             ← 전체 조회
```

`LPUSH`만 하면 같은 상품을 여러 번 본 경우 중복이 누적된다. `LREM`으로 기존 항목을 제거한 뒤 `LPUSH`하면 항상 최신 본 항목이 맨 앞에 위치한다.

---

## Session Store

[app/routes/session_store.py](app/routes/session_store.py)

단일 서버에서는 메모리에 세션을 저장해도 문제없다.
분산 서버 환경에서는 로드밸런서가 요청을 다른 서버로 보낼 수 있어, 서버마다 세션이 따로 관리되면 로그인이 풀리는 문제가 생긴다.

Sticky session(같은 유저는 같은 서버로)으로 우회할 수 있지만 세 가지 문제가 있다.
- 특정 서버에 트래픽 몰림
- 해당 서버 다운 시 세션 전부 소멸
- 무중단 배포가 어려움

### 해결

모든 서버가 Redis 하나를 바라보게 하면 문제가 해결된다.

```
SET session:{token} {data} EX 3600
GET session:{token}
DEL session:{token}
```

### JWT를 사용하더라도

JWT는 만료 전까지 서버가 강제로 무효화하기 어렵다. 로그아웃, 비밀번호 변경, 토큰 탈취 대응 같은 즉시 무효화가 필요한 경우 Redis에 블랙리스트나 화이트리스트를 두는 패턴이 일반적이다. refresh token 관리도 마찬가지로 Redis를 사용하는 경우가 많다.

### 참고 — Redis 고가용성

단일 Redis 인스턴스가 죽으면 세션이 전부 날아간다. 운영 환경에서는 아래 방식으로 대응한다.

| 방식 | 설명 |
|------|------|
| Redis Sentinel | 마스터 장애 시 자동 페일오버 |
| Redis Cluster | 데이터 샤딩 + 고가용성 |
| RDB | 주기적 스냅샷 저장 (배치성) |
| AOF | 매 쓰기 명령을 로그로 기록 (실시간) |

요즘은 RDB + AOF를 함께 사용한다. RDB로 오래된 스냅샷을, AOF로 최근 변경분을 복구한다.

---

## Article Stats

[app/routes/write_back.py](app/routes/write_back.py) · [app/tasks/flush_counts.py](app/tasks/flush_counts.py)

조회수와 좋아요 카운트처럼 빈번하게 변경되는 값을 Redis로 관리한다.
매 요청마다 DB에 UPDATE를 치는 대신, Redis에 INCR로 누적하고 주기적으로 한 번에 DB에 반영한다.

데이터 중요도에 따라 전략을 분리했다.

| 데이터 | 전략 | 이유 |
|--------|------|------|
| 조회수 카운트 | write-back | 중요도 낮음, 유실 허용 |
| 좋아요 매핑 (누가 눌렀는지) | write-through | 영구 데이터, 즉시 DB 반영 → 카운트 복구 기준점 |
| 좋아요 카운트 | write-back | article_likes COUNT(*)로 언제든 보정 가능 |

### 조회수

```
# 중복 방지: 오늘 이미 본 유저면 INCR 건너뜀
SISMEMBER article:{id}:viewer:{today} {user_id}

SADD      article:{id}:viewer:{today} {user_id}  EX 86400
INCR      article:{id}:view_count
SADD      dirty:view_articles {id}               ← flush 대상 등록
```

### 좋아요

좋아요 Set의 키를 article 기준이 아닌 유저 기준(`user:{id}:liked_articles`)으로 설계했다.

```
# 토글: 이미 눌렀으면 취소
SISMEMBER user:{user_id}:liked_articles {article_id}

SADD/SREM user:{user_id}:liked_articles {article_id}
INCR/DECR article:{id}:like_count
SADD      dirty:like_articles {id}               ← flush 대상 등록

# 매핑은 즉시 DB 반영 (write-through)
INSERT/DELETE article_likes
```

### Write-back flush (60초마다)

dirty set에 변경된 article_id를 모아뒀다가 한 번에 DB에 반영한다. Redis와 DB 사이에는 최대 flush 주기만큼의 지연(eventual consistency)이 생긴다.

SREM을 flush 이후에 하면, flush와 SREM 사이에 들어온 변경분의 article_id가 함께 제거되어 영구 유실된다 — flush 작업과 쓰기 요청 사이의 race condition이다. SREM을 먼저 하면 그 시점 이후 변경분은 다음 사이클의 dirty set에 새로 등록되어 처리된다.

```
dirty_ids = SMEMBERS dirty:view_articles
SREM dirty:view_articles {dirty_ids}   ← 먼저 제거
for id in dirty_ids:
    GET article:{id}:view_count → DB UPDATE
```

### Redis miss → DB warmup

Redis 키가 비어있는 상태(서버 재시작, eviction 등)에서 조회/좋아요 요청이 들어오면 카운트가 0부터 다시 시작되어 누적값이 깨진다. 키가 없을 때만 DB에서 현재 값을 불러와 Redis에 채워넣은 뒤 INCR/DECR한다.

```
EXISTS article:{id}:view_count → 없으면 SELECT view_count → SET
EXISTS user:{id}:liked_articles → 없으면 SELECT article_id FROM article_likes → SADD
EXISTS article:{id}:like_count → 없으면 SELECT like_count → SET
```

### 통계 조회

```
GET /article/{id}/stats
  └─ MGET article:{id}:view_count article:{id}:like_count
  └─ 둘 중 하나라도 miss면 DB에서 fallback + warmup
```

### Redis 장애 시

- 조회수: 유실 허용
- 좋아요 카운트: `article_likes` 테이블에서 `COUNT(*)`로 재계산 가능

---

## Verification Code

[app/routes/verification_code.py](app/routes/verification_code.py)

이메일/SMS 인증 번호는 일시적인 데이터라 DB에 저장할 필요가 없다.
TTL을 걸어 Redis에 저장하면 만료도 자동으로 처리된다.

전화번호를 그대로 키에 쓰면 Redis 키스페이스에 PII가 평문으로 남는다. SHA256 해시(`hashed_phone = sha256(phone)`)를 키로 사용한다.

```
SET auth:code:{hashed_phone} {code} EX 300   ← 300초 유효
```

### 원자성 문제

인증번호 검증 시 GET 후 맞으면 DEL하는 두 단계가 필요한데,
그 사이에 다른 요청이 끼어들면 같은 코드로 중복 인증이 가능해진다 — 전형적인 race condition이다.

`GETDEL`로 원자성을 보장할 수 있지만, 틀린 경우에도 코드가 삭제되어 UX가 나쁘다.

Lua 스크립트를 사용하면 GET → 비교 → 맞을 때만 DEL을 원자적으로 처리할 수 있다.

```lua
local stored = redis.call("GET", KEYS[1])
if not stored then return 0 end       -- 만료 또는 미발송
if stored == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1                          -- 인증 성공
end
return -1                             -- 코드 불일치
```

### 발송 제한

```
SET auth:limit:{hashed_phone} 1 NX EX 60   ← 60초 내 재발송 차단
```

### TODO

현재 `register_script`를 매 요청마다 호출하고 있다.
`register_script`는 Script 객체에 SHA를 캐싱해 EVALSHA로 실행하는 게 이점인데,
매번 새 객체를 생성하면 캐싱 이점이 없다.
앱 시작 시 한 번만 등록해서 재사용하도록 수정 필요.

---

## Distributed Lock

[app/routes/distributed_lock.py](app/routes/distributed_lock.py)

재고 차감처럼 동시 요청 간 race condition을 막아야 하는 작업에 사용한다.

### 1. SET NX 락

```
# 락 획득
SET lock:item:{item_id} {uuid} NX PX 5000

# 락 해제 (소유자 확인 후 DEL — Lua로 원자적 처리)
if GET lock:item:{item_id} == uuid → DEL lock:item:{item_id}
```

```lua
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
```

락 값으로 uuid를 저장해 소유자를 검증한다. 자신이 건 락만 해제할 수 있다.
TTL을 설정해두면 프로세스가 죽어도 락이 자동 해제되어 데드락을 방지한다.
`try/finally`로 예외 발생 시에도 락이 반드시 해제되도록 했다.

**문제 1 — 상호 배제 위반**: 작업이 TTL보다 오래 걸리면 락이 만료되어 다른 프로세스가 동시에 락을 획득할 수 있다.

**문제 2 — 공정성 없음**: 재시도가 무작위 폴링이라 먼저 기다린 요청이 먼저 락을 얻는다는 보장이 없다.

### 2. FIFO List Queue + SET NX 락

공정성 문제를 해결하기 위해 대기열을 추가했다.

```
RPUSH  queue:item:{item_id} {request_id}    ← 줄 서기
LINDEX queue:item:{item_id} 0               ← 내 차례 확인 (폴링)
LREM   queue:item:{item_id} 1 {request_id}  ← 타임아웃 시 스스로 제거
LPOP   queue:item:{item_id}                 ← 처리 완료 후 제거
```

타임아웃 시 `LREM`으로 스스로 큐에서 제거해 고아 항목을 방지했지만, 프로세스 크래시나 락 획득 직전 타이밍 이슈 같은 엣지 케이스는 여전히 남는다.

### 3. redis-py 내장 Lock

1번에서 직접 구현한 것들(uuid 소유자 검증, Lua 원자적 해제, 재시도 폴링, TTL, try/finally)이 내장 Lock 안에 이미 다 들어있다. 사실상 1번을 제대로 구현한 버전이다.

단, 작업이 TTL보다 오래 걸리는 상호 배제 위반 문제는 여전히 남는다. 필요 시 `lock.extend()`로 수동 연장할 수 있다.

```python
async with Lock(rd, lock_name, timeout=5, blocking_timeout=10): ...
```

---

## Rate Limiting

[app/middlewares/rate_limiting_fixed_window.py](app/middlewares/rate_limiting_fixed_window.py) · [app/middlewares/rate_limiting_sliding_window.py](app/middlewares/rate_limiting_sliding_window.py) · [app/middlewares/rate_limiting_token_bucket.py](app/middlewares/rate_limiting_token_bucket.py)

### Fixed Window

IP + 현재 윈도우 번호(`unix_time // 60`)를 키로 사용해 단순 카운팅한다.

```
INCR   rate_limit:{ip}:{current_minute}
EXPIRE rate_limit:{ip}:{current_minute} 60   ← count == 1일 때만
```

**문제 — 경계 버스트**: 58~59초에 5회, 0~1초에 5회 요청하면 2초 사이에 10회가 통과된다. 윈도우 경계에서 분당 제한이 의미없어진다.

### Sliding Window

고정 윈도우 대신 "지금으로부터 60초 이내" 요청만 카운팅한다.
Sorted Set에 요청 시각(unix time)을 score로 저장하고, 윈도우 밖 오래된 항목을 제거한 뒤 개수를 센다.

ZCARD 확인 → ZADD가 별도 명령이면 두 요청이 동시에 ZCARD를 읽고 둘 다 통과할 수 있다 (race condition). Lua 스크립트로 원자적으로 처리한다.

주요 연산 복잡도: ZADD O(log N), ZREMRANGEBYSCORE O(log N + M, M은 제거 원소 수), ZCARD O(1)

```lua
-- redis.call('TIME')는 {초, 마이크로초} 배열을 반환 — 산술 연산 전 tonumber 변환 필요
local now = ...  -- TIME 결과에서 변환
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)  -- 윈도우 밖 제거
local count = redis.call('ZCARD', key)
if count >= limit then return 0 end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, window + 5)
return 1
```

### Token Bucket

Sliding Window는 ZADD/ZREMRANGEBYSCORE가 O(log N)이라 요청이 많아질수록 무거워진다.

Token Bucket은 Hash로 구현해 HMGET/HSET이 O(1)이다. AWS API Gateway 등에서도 사용하는 방식이다.

버킷에 토큰을 채워두고 요청마다 1개씩 소비한다. 마지막 리필 시각과 경과 시간을 기준으로 토큰을 보충하므로, 안 쓴 시간만큼 모아서 버스트가 가능하다.

```lua
-- redis.call('TIME')는 {초, 마이크로초} 배열을 반환 — 산술 연산 전 tonumber 변환 필요
local now = ...  -- TIME 결과에서 변환
local stored = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens
if stored[1] == false then
    tokens = capacity                          -- 최초 진입: 가득 찬 버킷으로 시작
else
    local elapsed = now - tonumber(stored[2])
    tokens = math.min(capacity, tonumber(stored[1]) + elapsed * refill_rate)
end
if tokens < 1 then return 0 end
redis.call('HSET', key, 'tokens', tokens - 1, 'last_refill', now)
redis.call('EXPIRE', key, ttl)                 -- 유휴 키 자동 정리
return 1
```

서비스 성격에 따라 선택 기준이 달라진다.

| 상황 | 적합한 방식 |
|------|------------|
| 로그인, 결제 등 엄격한 제한 | Sliding Window |
| 검색, 일반 조회 등 버스트 허용 | Token Bucket |

---

## Leaderboard

[app/routes/rank_score.py](app/routes/rank_score.py)

DB에서 `ORDER BY score DESC`로 순위를 계산하면 전체 테이블 스캔 + 정렬이 필요하다.
일/주 단위 누적 점수를 DB에서 집계하려면 복잡한 쿼리나 별도 집계 테이블이 필요하다.

Sorted Set은 삽입 시점부터 정렬을 유지하고, `ZINCRBY`로 점수 누적이 간단하다.
키에 날짜를 포함하고 `EXPIREAT`으로 자정에 자동 만료시켜 일별 리더보드를 관리한다.

```
# 점수 누적
ZINCRBY  leaderboard:daily:{date} {score_delta} {user_id}
EXPIREAT leaderboard:daily:{date} {midnight}     ← 자정 자동 만료 (최초 1회)

# 상위 10명
ZRANGE leaderboard:daily:{date} 0 9 BYSCORE REV WITHSCORES

# 내 순위 / 내 주변 순위
ZREVRANK leaderboard:daily:{date} {user_id}
ZSCORE   leaderboard:daily:{date} {user_id}
```

---

## Pub/Sub

[app/routes/pub_sub.py](app/routes/pub_sub.py) · [app/routes/pub_sub_ws.py](app/routes/pub_sub_ws.py)

Redis Pub/Sub으로 publisher와 subscriber를 디커플링한다. publisher는 채널에 던지기만 하면 되고 누가 듣는지 알 필요가 없다.

| 기준 | SSE | WebSocket |
|------|-----|-----------|
| 방향 | 서버 → 클라이언트 단방향 | 양방향 |
| 프로토콜 | HTTP | HTTP 업그레이드 후 WS |
| 자동 재연결 | 브라우저가 자동 처리 | 직접 구현 필요 |
| 인프라 친화성 | 높음 (HTTP 그대로) | 프록시/방화벽 이슈 가능 |
| 적합한 상황 | 알림, 시세, 진행률 | 채팅, 협업, 실시간 게임 |

### SSE

```
POST /publish-notice  → PUBLISH system:notices {msg}
GET  /stream-notices  → SSE stream
```

### WebSocket

클라이언트도 메시지를 publish할 수 있다.

```
WS /ws/notices
  수신: receive_text() → PUBLISH system:notices:ws {msg}
  송신: queue → send_text()
```

### 문제 — 클라이언트마다 Redis 구독 생성

최초 구현에서 클라이언트 연결마다 Redis pubsub 객체를 생성하면 Redis 커넥션이 클라이언트 수만큼 늘어난다. Redis 입장에서는 커넥션 수 부담, 서버 입장에서는 구독 태스크 수 부담이 그대로 쌓인다.

### Fan-out 구조

서버 구동 시 Redis 리스너를 하나만 백그라운드 태스크로 실행한다.
클라이언트 연결마다 `asyncio.Queue`를 만들어 공유 `set`에 등록하고, 리스너가 메시지를 받으면 모든 큐에 fan-out한다.

```
Redis listener (서버당 1개)
  메시지 수신 → for q in subscribers: q.put_nowait(msg)
                  ├─ queue[client A] → SSE
                  ├─ queue[client B] → SSE
                  └─ queue[client C] → WS

클라이언트 연결 시: queue 생성 → subscribers.add(queue)
클라이언트 해제 시: subscribers.discard(queue)
```

큐가 가득 찬 클라이언트는 오래된 메시지를 drop하고 종료 신호(`None`)를 전송한다.

`put_nowait`(non-blocking)을 사용하는 이유는, blocking `put`을 쓰면 느린 클라이언트 하나의 큐가 찼을 때 리스너 전체가 블록되어 다른 모든 클라이언트도 메시지를 못 받기 때문이다.

drop-oldest를 선택한 이유는, 실시간 공지 시스템에서는 오래된 메시지보다 최신 메시지가 더 중요하기 때문이다.

구독자를 `set[asyncio.Queue]`로 관리하는 이유는 두 가지다.
- **`set`**: 클라이언트 연결/해제 시 `add`/`discard`가 O(1). fan-out은 순서가 중요하지 않아 list보다 적합하다. 순회 중 set이 변경될 수 있어 `list(subscribers)`로 복사한 뒤 순회한다.
- **클라이언트마다 독립 Queue**: 리스너가 SSE/WS 객체에 직접 쓰면 느린 클라이언트에서 블록될 수 있다. Queue로 분리하면 리스너는 `put_nowait`만 하고 끝나고, 각 클라이언트가 자신의 Queue에서 독립적으로 소비한다.
