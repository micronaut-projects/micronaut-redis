from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from jakarta.inject import Inject, Singleton


@Singleton
class RedisClientCommands:

    # tag::commands[]
    connection: Annotated[StatefulRedisConnection[str, str], Inject]

    def commands(self) -> None:
        commands = self.connection.sync()
        commands.set("foo", "bar")
        commands.get("foo")
    # end::commands[]
