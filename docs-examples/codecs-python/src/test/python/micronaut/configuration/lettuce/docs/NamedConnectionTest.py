from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from jakarta.inject import Inject, Named
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .NamedConnectionCommands import NamedConnectionCommands


@Property(name="spec.name", value="NamedConnectionTest")
@MicronautTest
class NamedConnectionTest:
    named_connection_commands: Annotated[NamedConnectionCommands, Inject]
    foo_connection: Annotated[StatefulRedisConnection[str, str], Inject, Named("foo")]

    @Test
    def test_named_connection(self):
        self.named_connection_commands.commands()

        assert self.foo_connection.sync().get("named") == "foo"
