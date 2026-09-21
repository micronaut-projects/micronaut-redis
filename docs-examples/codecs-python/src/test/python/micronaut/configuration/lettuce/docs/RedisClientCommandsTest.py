from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from jakarta.inject import Inject
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .RedisClientCommands import RedisClientCommands


@MicronautTest
class RedisClientCommandsTest:
    redis_client_commands: Annotated[RedisClientCommands, Inject]
    connection: Annotated[StatefulRedisConnection[str, str], Inject]

    @Test
    def test_commands(self):
        self.redis_client_commands.commands()

        assert self.connection.sync().get("foo") == "bar"
