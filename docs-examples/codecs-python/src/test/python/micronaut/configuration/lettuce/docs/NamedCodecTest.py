from typing import Annotated

from jakarta.inject import Inject, Named
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .JavaBytes import java_bytes

try:
    from io.lettuce.core.api import StatefulRedisConnection
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.api import StatefulRedisConnection


@Property(name="spec.name", value="NamedCodecTest")
@MicronautTest
class NamedCodecTest:
    # TODO(python): bytes as a generic type argument is mapped to java.lang.Byte instead of byte[],
    # consistently with the RedisCodec[bytes, bytes] bean of NamedCodecFactory
    foo_connection: Annotated[StatefulRedisConnection[bytes, bytes], Inject, Named("foo")]
    bar_connection: Annotated[StatefulRedisConnection[str, str], Inject, Named("bar")]

    @Test
    def test_named_codecs(self):
        key = java_bytes("foo-key")
        value = java_bytes("foo-value")
        foo_commands = self.foo_connection.sync()
        foo_commands.set(key, value)
        bar_commands = self.bar_connection.sync()
        bar_commands.set("bar-key", "bar-value")

        assert list(foo_commands.get(key)) == list(value)
        assert bar_commands.get("bar-key") == "bar-value"
