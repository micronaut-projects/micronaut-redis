from typing import Annotated

from jakarta.inject import Inject, Singleton
from java.util.concurrent import TimeUnit
from micronaut.context.annotation import Requires

try:
    from io.lettuce.core.api import StatefulRedisConnection
    from io.lettuce.core.support import AsyncPool
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.api import StatefulRedisConnection
    from lettuce.core.support import AsyncPool


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
