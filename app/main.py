import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import redis.asyncio as redis

from db.session import engine, Base
import db.models  # noqa: F401 — ensures models are registered before create_all

from middlewares import (
    rate_limit_fixed_window,
    rate_limit_sliding_window,
    rate_limit_token_bucket,
)
from routes import (
    cache_aside,
    recent_list,
    session_store,
    write_back,
    verification_code,
    distributed_lock,
    rank_score,
    pub_sub,
    pub_sub_ws,
)
from tasks.flush_counts import flush_view_counts, flush_like_counts


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    app.state.redis = redis.from_url("redis://redis:6379/0", decode_responses=True)
    await app.state.redis.ping()
    print("Redis Connected.")

    app.state.subscribers = set()
    app.state.ws_subscribers = set()

    view_task = asyncio.create_task(flush_view_counts(app.state.redis))
    like_task = asyncio.create_task(flush_like_counts(app.state.redis))
    pub_sub_task = asyncio.create_task(
        pub_sub.redis_listener(app.state.redis, app.state.subscribers)
    )
    pub_sub_ws_task = asyncio.create_task(
        pub_sub_ws.redis_listener(app.state.redis, app.state.ws_subscribers)
    )

    yield

    view_task.cancel()
    like_task.cancel()
    pub_sub_task.cancel()
    pub_sub_ws_task.cancel()

    # 태스크가 완전히 종료될 때까지 대기 (Redis 닫기 전에 보장)
    # return_exceptions=True: CancelledError를 예외로 올리지 않고 반환값으로 처리
    await asyncio.gather(
        view_task, like_task, pub_sub_task, pub_sub_ws_task, return_exceptions=True
    )

    await app.state.redis.aclose()
    print("Redis Disconnected..")


app = FastAPI(lifespan=lifespan)

# app.middleware("http")(rate_limit_fixed_window)
# app.middleware("http")(rate_limit_sliding_window)
# app.middleware("http")(rate_limit_token_bucket)

app.include_router(cache_aside.router, tags=["cache-aside"])
app.include_router(recent_list.router, tags=["recent-list"])
app.include_router(session_store.router, tags=["session-store"])
app.include_router(write_back.router, tags=["write-back"])
app.include_router(verification_code.router, tags=["verification-code"])
app.include_router(distributed_lock.router, tags=["distributed-lock"])
app.include_router(rank_score.router, tags=["rank-score"])
app.include_router(pub_sub.router, tags=["pub-sub"])
app.include_router(pub_sub_ws.router, tags=["pub-sub-ws"])
