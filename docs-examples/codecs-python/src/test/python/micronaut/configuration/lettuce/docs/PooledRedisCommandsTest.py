from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from jakarta.inject import Inject
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .PooledRedisCommands import PooledRedisCommands


@Property(name="redis.pool.enabled", value="true")
@MicronautTest
class PooledRedisCommandsTest:
    pooled_redis_commands: Annotated[PooledRedisCommands, Inject]
    connection: Annotated[StatefulRedisConnection[str, str], Inject]

    @Test
    def test_pooled_commands(self):
        self.pooled_redis_commands.commands()

        assert self.connection.sync().get("first") == "one"
