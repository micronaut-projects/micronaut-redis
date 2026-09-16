from typing import Annotated

from jakarta.inject import Inject, Singleton

try:
    from io.lettuce.core.api import StatefulRedisConnection
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.api import StatefulRedisConnection


@Singleton
class RedisClientCommands:

    # tag::commands[]
    connection: Annotated[StatefulRedisConnection[str, str], Inject]

    def commands(self) -> None:
        commands = self.connection.sync()
        commands.set("foo", "bar")
        commands.get("foo")
    # end::commands[]
