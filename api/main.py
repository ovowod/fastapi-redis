from contextlib import asynccontextmanager
from fastapi import FastAPI
import redis.asyncio as redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url("redis://redis:6379/0", decode_responses=True)
    await app.state.redis.ping()
    print("Redis Connected.")

    yield

    await app.state.redis.aclose()
    print("Redis Disconnected..")


app = FastAPI(lifespan=lifespan)
