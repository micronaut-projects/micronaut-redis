from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from io.lettuce.core.support import AsyncPool
from jakarta.inject import Inject, Singleton
from java.util.concurrent import TimeUnit
from micronaut.context.annotation import Requires


@Requires(property="redis.pool.enabled", value="true")
@Singleton
class PooledRedisCommands:

    # tag::pool[]
    pool: Annotated[AsyncPool[StatefulRedisConnection[str, str]], Inject]

    def commands(self) -> None:
        connection = self.pool.acquire().get(5, TimeUnit.SECONDS)
        try:
            connection.sync().set("first", "one")
        finally:
            self.pool.release(connection).get(5, TimeUnit.SECONDS)
    # end::pool[]
