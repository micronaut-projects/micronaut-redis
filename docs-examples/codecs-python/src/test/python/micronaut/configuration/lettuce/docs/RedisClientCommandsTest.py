from typing import Annotated

from jakarta.inject import Inject
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .RedisClientCommands import RedisClientCommands

try:
    from io.lettuce.core.api import StatefulRedisConnection
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.api import StatefulRedisConnection


@MicronautTest
class RedisClientCommandsTest:
    redis_client_commands: Annotated[RedisClientCommands, Inject]
    connection: Annotated[StatefulRedisConnection[str, str], Inject]

    @Test
    def test_commands(self):
        self.redis_client_commands.commands()

        assert self.connection.sync().get("foo") == "bar"
