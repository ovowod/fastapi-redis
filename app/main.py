from contextlib import asynccontextmanager
from fastapi import FastAPI
import redis.asyncio as redis

from db.session import engine, Base
import db.models  # noqa: F401 — ensures models are registered before create_all

from routes import cache_aside, recent_list, session_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    app.state.redis = redis.from_url("redis://redis:6379/0", decode_responses=True)
    await app.state.redis.ping()
    print("Redis Connected.")

    yield

    await app.state.redis.aclose()
    print("Redis Disconnected..")


app = FastAPI(lifespan=lifespan)

app.include_router(cache_aside.router, tags=["cache-aside"])
app.include_router(recent_list.router, tags=["recent-list"])
app.include_router(session_store.router, tags=["session-store"])
