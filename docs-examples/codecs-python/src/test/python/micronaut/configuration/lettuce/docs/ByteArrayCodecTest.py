from typing import Annotated

from io.lettuce.core.api import StatefulRedisConnection
from io.lettuce.core.codec import ByteArrayCodec, RedisCodec
from jakarta.inject import Inject
from java.nio import ByteBuffer
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test


@Property(name="spec.name", value="ByteArrayCodecTest")
@MicronautTest
class ByteArrayCodecTest:
    codec: Annotated[RedisCodec[bytes, bytes], Inject]

    # tag::typedConnection[]
    connection: Annotated[StatefulRedisConnection[bytes, bytes], Inject]
    # end::typedConnection[]

    @Test
    def test_byte_array_codec(self):
        assert self.codec == ByteArrayCodec.INSTANCE

        key = ByteBuffer.wrap(b"bytes").array()
        value = ByteBuffer.wrap(b"value").array()
        commands = self.connection.sync()
        commands.set(key, value)

        assert list(commands.get(key)) == list(value)
