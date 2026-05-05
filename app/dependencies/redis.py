import redis.asyncio as redis
from starlette.requests import HTTPConnection


# HTTPConnection: Request(HTTP)와 WebSocket 공통 부모 → 두 엔드포인트 모두에서 주입 가능
def get_redis(conn: HTTPConnection) -> redis.Redis:
    return conn.app.state.redis
