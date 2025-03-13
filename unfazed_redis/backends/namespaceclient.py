from redis.asyncio import Redis
import typing as t
from unfazed_redis.schema.option import RedisOptions
from redis.asyncio.connection import parse_url
from redis.backoff import ConstantBackoff
from redis.asyncio.retry import Retry
from redis.exceptions import DataError


def default_key_func(key: str, key_prefix: str | None, version: str | None) -> str:
    return "%s:%s:%s" % (key_prefix or "default", version or "1", key)


class NamespaceClient:
    def __init__(self, location: str, options: t.Dict[str, t.Any] | None = None):
        self._client = None
        options = options or {}
        self.options = RedisOptions(**options)
        kw = parse_url(location)

        if self.options.retry:
            retry_cls = Retry(ConstantBackoff(0.5), 1)
        else:
            retry_cls = None
        self.client = Redis(
            host=kw.get("host", "localhost"),
            port=kw.get("port", 6379),
            db=kw.get("db", 0),
            password=kw.get("password", None),
            username=kw.get("username"),
            retry=retry_cls,
            socket_timeout=self.options.socket_timeout,
            socket_connect_timeout=self.options.socket_connect_timeout,
            socket_keepalive=self.options.socket_keepalive,
            socket_keepalive_options=self.options.socket_keepalive_options,
            decode_responses=self.options.decode_responses,
            retry_on_timeout=self.options.retry_on_timeout,
            retry_on_error=self.options.retry_on_error,
            max_connections=self.options.max_connections,
            single_connection_client=self.options.single_connection_client,
            health_check_interval=self.options.health_check_interval,
            ssl=self.options.ssl,
            ssl_keyfile=self.options.ssl_keyfile,
            ssl_certfile=self.options.ssl_certfile,
            ssl_cert_reqs=self.options.ssl_cert_reqs,
            ssl_ca_certs=self.options.ssl_ca_certs,
            ssl_ca_data=self.options.ssl_ca_data,
            ssl_check_hostname=self.options.ssl_check_hostname,
            ssl_min_version=self.options.ssl_min_version,
            ssl_ciphers=self.options.ssl_ciphers,
        )

    def make_key(self, key: str) -> str:
        prefix = self.options.prefix
        version = self.options.version
        return default_key_func(key, prefix, version)

    async def get(self, key: str) -> t.Any:
        full_key = self.make_key(key)
        return await self.client.get(full_key)

    async def set(self, key: str, value: t.Any, timeout: int | None = None) -> None:
        full_key = self.make_key(key)
        await self.client.set(full_key, value, ex=timeout)

    async def delete(self, *keys: t.Any) -> int:
        full_keys = [self.make_key(key) for key in keys]
        return await self.client.delete(*full_keys)

    async def exists(self, *keys: t.Any) -> int:
        full_keys = [self.make_key(key) for key in keys]
        return await self.client.exists(*full_keys)

    async def expire(self, key: str, seconds: int) -> bool:
        full_key = self.make_key(key)
        return await self.client.expire(full_key, seconds)

    async def ttl(self, key: str) -> int:
        full_key = self.make_key(key)
        return await self.client.ttl(full_key)

    async def hdel(self, key: str, *args: t.Any) -> int:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hdel(full_key, *args))

    async def hexists(self, key: str, field: str) -> bool:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hexists(full_key, field))

    async def hget(self, key: str, *args: t.Any) -> t.Any:
        full_key = self.make_key(key)
        return await self.client.hget(full_key, *args)  # type: ignore

    async def hgetall(self, key: str) -> t.Dict[str, t.Any]:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hgetall(full_key))

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.hincrby(full_key, field, increment)
        )

    async def hkeys(self, key: str) -> t.List[str]:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hkeys(full_key))

    async def hlen(self, key: str) -> int:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hlen(full_key))

    async def hmget(self, key: str, *args: t.Any) -> t.List[t.Any]:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hmget(full_key, *args))

    async def hmset(self, key: str, mapping: t.Dict[str, t.Any]) -> None:
        """Set multiple hash fields to multiple values."""
        if mapping is None:
            raise DataError("'hmset' value cannot be None")
        if not isinstance(mapping, dict):
            raise DataError("'hmset' value must be a dict")
        if not mapping:
            return  # 如果是空字典，直接返回
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hmset(full_key, mapping))

    async def hset(
        self,
        key: str,
        field: t.Any = None,
        value: t.Any = None,
        mapping: t.Dict[str, t.Any] | None = None,
        items: t.Dict[str, t.Any] | None = None,
    ) -> int:
        """Set field/value into hash, and also accept a mapping to batch set."""
        full_key = self.make_key(key)
        if items:
            mapping = dict(items)
        if field is None and not mapping:
            raise DataError("'hset' with no key value pairs")
        return await t.cast(
            t.Awaitable[t.Any], self.client.hset(full_key, field, value, mapping)
        )

    async def hsetnx(self, key: str, field: t.Any, value: t.Any) -> bool:
        """Set field/value into hash only if field does not exist."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.hsetnx(full_key, field, value)
        )

    async def hvals(self, key: str) -> t.List[t.Any]:
        """Return all values in hash."""
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hvals(full_key))

    async def hscan(
        self, key: str, cursor: int = 0, match: t.Any = None, count: int | None = None
    ) -> t.Tuple[int, t.Dict]:
        """Incrementally iterate hash fields and values."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.hscan(full_key, cursor, match, count)
        )

    async def hrandfield(
        self, key: str, count: int, withvalues: bool = False
    ) -> t.Union[str, t.List]:
        """Return random field from hash."""
        full_key = self.make_key(key)
        return await self.client.hrandfield(full_key, count, withvalues)

    async def hstrlen(self, key: str, field: t.Any) -> int:
        """Return length of string value in hash."""
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.hstrlen(full_key, field))

    async def lpush(self, key: str, *values: t.Any) -> int:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.lpush(full_key, *values))

    async def rpush(self, key: str, *values: t.Any) -> int:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.rpush(full_key, *values))

    async def lpop(self, key: str) -> t.Any:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.lpop(full_key))

    async def rpop(self, key: str) -> t.Any:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.rpop(full_key))

    async def lrange(self, key: str, start: int, end: int) -> t.List[t.Any]:
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.lrange(full_key, start, end)
        )

    async def sadd(self, key: str, *members: t.Any) -> int:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.sadd(full_key, *members))

    async def srem(self, key: str, *members: t.Any) -> int:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.srem(full_key, *members))

    async def spop(
        self, key: str, count: int | None = None
    ) -> t.Union[str, t.List[str]]:
        """Remove and return random members from set."""
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.spop(full_key, count))

    async def scard(self, key: str) -> int:
        """Get the number of members in a set."""
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.scard(full_key))

    async def smembers(self, key: str) -> t.Set[t.Any]:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.smembers(full_key))

    async def sismember(self, key: str, member: t.Any) -> bool:
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.sismember(full_key, member))

    async def smismember(self, key: str, members: t.List[t.Any]) -> t.List[bool]:
        """Check if multiple values are members of a set."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.smismember(full_key, members)
        )

    async def srandmember(
        self, key: str, number: int | None = None
    ) -> t.Union[str, t.List[str]]:
        """Get random members from set."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.srandmember(full_key, number)
        )

    async def sscan(
        self, key: str, cursor: int = 0, match: t.Any = None, count: int | None = None
    ) -> t.Tuple[int, t.List[str]]:
        """Incrementally iterate Set elements."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.sscan(full_key, cursor, match, count)
        )

    async def sinter(self, *keys: str) -> t.Set[t.Any]:
        """获取多个集合的交集"""
        full_keys = [self.make_key(key) for key in keys]
        return await t.cast(t.Awaitable[t.Any], self.client.sinter(full_keys))

    async def sinterstore(self, dest: str, keys: t.List[str], *args) -> int:
        """Store the intersection of sets into a new set."""
        full_dest = self.make_key(dest)
        full_keys = [self.make_key(key) for key in keys]
        return await t.cast(
            t.Awaitable[t.Any], self.client.sinterstore(full_dest, full_keys, *args)
        )

    async def sunion(self, *keys: str) -> t.Set[t.Any]:
        """获取多个集合的并集"""
        full_keys = [self.make_key(key) for key in keys]
        return await t.cast(t.Awaitable[t.Any], self.client.sunion(full_keys))

    async def sunionstore(self, dest: str, keys: t.List[str], *args: t.Any) -> int:
        """Store the union of sets into a new set."""
        full_dest = self.make_key(dest)
        full_keys = [self.make_key(key) for key in keys]
        return await t.cast(
            t.Awaitable[t.Any], self.client.sunionstore(full_dest, full_keys, *args)
        )

    async def sdiff(self, *keys: str) -> t.Set[t.Any]:
        """获取多个集合的差集"""
        full_keys = [self.make_key(key) for key in keys]
        return await t.cast(t.Awaitable[t.Any], self.client.sdiff(full_keys))

    async def sdiffstore(self, dest: str, keys: t.List[str], *args: t.Any) -> int:
        """Store the difference of sets into a new set."""
        full_dest = self.make_key(dest)
        full_keys = [self.make_key(key) for key in keys]
        return await t.cast(
            t.Awaitable[t.Any], self.client.sdiffstore(full_dest, full_keys, *args)
        )

    async def smove(self, src: str, dst: str, value: t.Any) -> bool:
        """Move member from one set to another."""
        full_src = self.make_key(src)
        full_dst = self.make_key(dst)
        return await t.cast(
            t.Awaitable[t.Any], self.client.smove(full_src, full_dst, value)
        )

    async def zadd(self, key: str, mapping: t.Dict[t.Any, float]) -> int:
        full_key = self.make_key(key)
        return await self.client.zadd(full_key, mapping)

    async def zrem(self, key: str, *members: t.Any) -> int:
        full_key = self.make_key(key)
        return await self.client.zrem(full_key, *members)

    async def zrange(self, key: str, start: int, stop: int, desc: bool = False) -> list:
        """获取有序集合中指定范围的成员

        Args:
            key: 键名
            start: 起始位置
            stop: 结束位置
            desc: 是否按分数从高到低排序
        """
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.zrange(full_key, start, stop, desc=desc)
        )

    async def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """Count elements with scores within the given values."""
        full_key = self.make_key(key)
        return await self.client.zcount(full_key, min_score, max_score)

    async def zscore(self, key: str, member) -> float:
        """Get the score of member in sorted set."""
        full_key = self.make_key(key)
        return await self.client.zscore(full_key, member)

    async def zincrby(self, key: str, amount: float, member) -> float:
        """Increment the score of member in sorted set by amount."""
        full_key = self.make_key(key)
        return await self.client.zincrby(full_key, amount, member)

    async def zrank(self, key: str, member: t.Any) -> int:
        """Get index of member in sorted set (scores low to high)."""
        full_key = self.make_key(key)
        return await self.client.zrank(full_key, member)

    async def zrevrank(self, key: str, member: t.Any) -> int:
        """Get index of member in sorted set (scores high to low)."""
        full_key = self.make_key(key)
        return await self.client.zrevrank(full_key, member)

    async def zrangebyscore(
        self,
        key: str,
        min_score: t.Union[float, str],
        max_score: t.Union[float, str],
        start: t.Optional[int] = None,
        num: t.Optional[int] = None,
        withscores: bool = False,
    ) -> t.List[t.Any]:
        """Return members with scores between min and max."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any],
            self.client.zrangebyscore(
                full_key, min_score, max_score, start, num, withscores
            ),
        )

    async def zrevrangebyscore(
        self,
        key: str,
        max_score: t.Union[float, str],
        min_score: t.Union[float, str],
        start: t.Optional[int] = None,
        num: t.Optional[int] = None,
        withscores: bool = False,
    ) -> t.List[t.Any]:
        """Return members with scores between max and min, in reverse order."""
        full_key = self.make_key(key)
        return await self.client.zrevrangebyscore(
            full_key, max_score, min_score, start, num, withscores
        )

    async def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set."""
        full_key = self.make_key(key)
        return await t.cast(t.Awaitable[t.Any], self.client.zcard(full_key))

    async def zremrangebyrank(self, key: str, start: int, end: int) -> int:
        """Remove members in sorted set with ranks between start and end."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any], self.client.zremrangebyrank(full_key, start, end)
        )

    async def zremrangebyscore(
        self, key: str, min_score: float, max_score: float
    ) -> int:
        """Remove members in sorted set with scores between min and max."""
        full_key = self.make_key(key)
        return await t.cast(
            t.Awaitable[t.Any],
            self.client.zremrangebyscore(full_key, min_score, max_score),
        )

    async def zrandmember(
        self, key: str, count: t.Optional[int] = None, withscores: bool = False
    ) -> t.Union[str, t.List[t.Any]]:
        """Get random members from sorted set."""
        full_key = self.make_key(key)
        return await self.client.zrandmember(full_key, t.cast(int, count), withscores)

    async def incr(self, key: str) -> int:
        full_key = self.make_key(key)
        return await self.client.incr(full_key)

    async def decr(self, key: str) -> int:
        full_key = self.make_key(key)
        return await self.client.decr(full_key)

    async def incrby(self, key: str, amount: int) -> int:
        full_key = self.make_key(key)
        return await self.client.incrby(full_key, amount)

    async def decrby(self, key: str, amount: int) -> int:
        full_key = self.make_key(key)
        return await self.client.decrby(full_key, amount)

    async def pipeline(self, transaction: bool = False, shard_hint: t.Any = None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. transaction defaults to False since Codis does not support it.
        """
        return self.client.pipeline(transaction=transaction, shard_hint=shard_hint)

    async def zrevrange(
        self,
        key: str,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: t.Callable = float,
    ) -> t.List[t.Any]:
        """获取有序集合中指定范围的成员（按分数从高到低排序）

        Args:
            key: 键名
            start: 起始位置
            end: 结束位置
            withscores: 是否返回分数
            score_cast_func: 分数转换函数

        Returns:
            如果 withscores 为 False，返回成员列表
            如果 withscores 为 True，返回 (成员, 分数) 元组的列表
        """
        full_key = self.make_key(key)
        return await self.client.zrevrange(
            full_key, start, end, withscores=withscores, score_cast_func=score_cast_func
        )
