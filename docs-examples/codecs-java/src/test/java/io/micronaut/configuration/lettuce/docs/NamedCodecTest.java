package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Property(name = "spec.name", value = "NamedCodecTest")
@MicronautTest
class NamedCodecTest extends AbstractRedisTest {

    @Inject
    @Named("foo")
    StatefulRedisConnection<byte[], byte[]> fooConnection;

    @Inject
    @Named("bar")
    StatefulRedisConnection<String, String> barConnection;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>(super.getProperties());
        properties.put("redis.servers.foo.uri", properties.get("redis.uri"));
        properties.put("redis.servers.bar.uri", properties.get("redis.uri"));
        return properties;
    }

    @Test
    void testNamedCodecs() {
        byte[] key = "foo-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "foo-value".getBytes(StandardCharsets.UTF_8);
        RedisCommands<byte[], byte[]> fooCommands = fooConnection.sync();
        fooCommands.set(key, value);
        RedisCommands<String, String> barCommands = barConnection.sync();
        barCommands.set("bar-key", "bar-value");

        assertArrayEquals(value, fooCommands.get(key));
        assertEquals("bar-value", barCommands.get("bar-key"));
    }
}
