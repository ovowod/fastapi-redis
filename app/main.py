import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import redis.asyncio as redis

from db.session import engine, Base
import db.models  # noqa: F401 — ensures models are registered before create_all

from routes import (
    cache_aside,
    recent_list,
    session_store,
    write_back,
    verification_code,
)
from tasks.flush_counts import flush_view_counts, flush_like_counts


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    app.state.redis = redis.from_url("redis://redis:6379/0", decode_responses=True)
    await app.state.redis.ping()
    print("Redis Connected.")

    view_task = asyncio.create_task(flush_view_counts(app.state.redis))
    like_task = asyncio.create_task(flush_like_counts(app.state.redis))

    yield

    view_task.cancel()
    like_task.cancel()

    await app.state.redis.aclose()
    print("Redis Disconnected..")


app = FastAPI(lifespan=lifespan)

app.include_router(cache_aside.router, tags=["cache-aside"])
app.include_router(recent_list.router, tags=["recent-list"])
app.include_router(session_store.router, tags=["session-store"])
app.include_router(write_back.router, tags=["write-back"])
app.include_router(verification_code.router, tags=["verification-code"])
