package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Property(name = "spec.name", value = "ByteArrayCodecTest")
@MicronautTest
class ByteArrayCodecTest extends AbstractRedisTest {

    @Inject
    RedisCodec<byte[], byte[]> codec;

    // tag::typedConnection[]
    @Inject
    StatefulRedisConnection<byte[], byte[]> connection;
    // end::typedConnection[]

    @Test
    void testByteArrayCodec() {
        assertSame(ByteArrayCodec.INSTANCE, codec);

        byte[] key = "bytes".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        RedisCommands<byte[], byte[]> commands = connection.sync();
        commands.set(key, value);

        assertArrayEquals(value, commands.get(key));
    }
}
