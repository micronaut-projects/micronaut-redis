from typing import Annotated

from jakarta.inject import Inject
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .JavaBytes import java_bytes

try:
    from io.lettuce.core.api import StatefulRedisConnection
    from io.lettuce.core.codec import ByteArrayCodec, RedisCodec
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.api import StatefulRedisConnection
    from lettuce.core.codec import ByteArrayCodec, RedisCodec


@Property(name="spec.name", value="ByteArrayCodecTest")
@MicronautTest
class ByteArrayCodecTest:
    # TODO(python): bytes as a generic type argument is mapped to java.lang.Byte instead of byte[],
    # consistently with the RedisCodec[bytes, bytes] bean of ByteArrayCodecFactory and with the
    # connection registered for that codec's type arguments
    codec: Annotated[RedisCodec[bytes, bytes], Inject]

    # tag::typedConnection[]
    connection: Annotated[StatefulRedisConnection[bytes, bytes], Inject]
    # end::typedConnection[]

    @Test
    def test_byte_array_codec(self):
        assert self.codec == ByteArrayCodec.INSTANCE

        key = java_bytes("bytes")
        value = java_bytes("value")
        commands = self.connection.sync()
        commands.set(key, value)

        assert list(commands.get(key)) == list(value)
