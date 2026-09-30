from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from jakarta.inject import Inject, Named, Singleton
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="NamedConnectionTest")
@Singleton
class NamedConnectionCommands:

    # tag::namedConnection[]
    connection: Annotated[StatefulRedisConnection[str, str], Inject, Named("foo")]
    # end::namedConnection[]

    def commands(self) -> None:
        self.connection.sync().set("named", "foo")
