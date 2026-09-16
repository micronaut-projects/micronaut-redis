package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

import java.nio.charset.StandardCharsets

@Property(name = "spec.name", value = "ByteArrayCodecTest")
@MicronautTest
class ByteArrayCodecSpec extends AbstractRedisTest {

    @Inject
    RedisCodec<byte[], byte[]> codec

    // tag::typedConnection[]
    @Inject
    StatefulRedisConnection<byte[], byte[]> connection
    // end::typedConnection[]

    void "test byte array codec"() {
        given:
        byte[] key = "bytes".getBytes(StandardCharsets.UTF_8)
        byte[] value = "value".getBytes(StandardCharsets.UTF_8)
        RedisCommands<byte[], byte[]> commands = connection.sync()

        when:
        commands.set(key, value)

        then:
        codec.is(ByteArrayCodec.INSTANCE)
        commands.get(key) == value
    }
}
