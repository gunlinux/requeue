import logging
import typing
from abc import ABC, abstractmethod
from types import TracebackType
from typing import override

from coredis import Redis
from coredis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from coredis.exceptions import (
    TimeoutError as RedisTimeoutError,
)

logger = logging.getLogger("rredis")


class BotConnectionError(Exception):
    """
    This is the way
    """


class Connection(ABC):
    @abstractmethod
    async def push(self, name: str, data: str) -> None: ...

    @abstractmethod
    async def pop(self, name: str) -> str: ...

    @abstractmethod
    async def llen(self, name: str) -> int: ...

    @abstractmethod
    async def walk(self, name: str) -> list[str]: ...

    @abstractmethod
    async def clean(self, name: str) -> None: ...

    @abstractmethod
    async def _connect(self) -> None: ...

    @abstractmethod
    async def _close(self) -> None: ...

    async def __aenter__(self) -> "Connection":
        await self._connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._close()


class RedisConnection(Connection):
    def __init__(self, url: str) -> None:
        self.url: str = url
        self._redis: Redis[str] | None = None

    @override
    async def _connect(self):
        if self._redis is None:
            self._redis = await Redis.from_url(
                self.url,
                decode_responses=True,
            )

    @override
    async def _close(self):
        self._redis = None

    async def call(
        self, action: str, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any:
        if self._redis is None:
            raise BotConnectionError
        func = getattr(self, action)
        try:
            return await func(self._redis, *args, **kwargs)
        except (RedisConnectionError, RedisTimeoutError) as e:
            logger.critical("cant push no redis conn, %s", e)

    async def _push(self, redis: Redis[str], name: str, data: str) -> None:
        _ = await redis.rpush(name, [data])

    @override
    async def push(self, name: str, data: str) -> None:
        return await self.call("_push", name, data)

    async def _pop(self, redis: Redis[str], name: str) -> str:
        temp: str | None = await redis.lpop(name)
        if temp and isinstance(temp, bytes):
            return temp.decode("utf-8")
        return typing.cast("str", temp)

    @override
    async def pop(self, name: str) -> str:
        return await self.call("_pop", name)

    async def _llen(self, redis: Redis[str], name: str) -> int:
        return await redis.llen(name)

    @override
    async def llen(self, name: str) -> int:
        return await self.call("_llen", name)

    async def _walk(self, redis: Redis[str], name: str) -> list[str]:
        return await redis.lrange(name, 0, -1)

    @override
    async def walk(self, name: str) -> list[str]:
        return await self.call("_walk", name=name)

    async def _clean(self, redis: Redis[str], name: str) -> None:
        await redis.delete([name])

    @override
    async def clean(self, name: str) -> None:
        return await self.call("_clean", name=name)
