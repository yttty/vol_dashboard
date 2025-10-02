from redis import Redis


def get_redis_instance(redis_url: str = "") -> Redis:
    if not redis_url:
        redis_url = "redis://localhost:6379/0"
    return Redis.from_url(redis_url, decode_responses=True)
