from typing import Annotated

from jakarta.inject import Inject, Named, Singleton
from micronaut.context.annotation import Requires

try:
    from io.lettuce.core.api import StatefulRedisConnection
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.api import StatefulRedisConnection


@Requires(property="spec.name", value="NamedConnectionTest")
@Singleton
class NamedConnectionCommands:

    # tag::namedConnection[]
    connection: Annotated[StatefulRedisConnection[str, str], Inject, Named("foo")]
    # end::namedConnection[]

    def commands(self) -> None:
        self.connection.sync().set("named", "foo")
