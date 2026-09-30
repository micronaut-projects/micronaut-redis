package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@MicronautTest
class RedisClientCommandsTest extends AbstractRedisTest {

    @Inject
    RedisClientCommands redisClientCommands;

    @Inject
    StatefulRedisConnection<String, String> connection;

    @Test
    void testCommands() {
        redisClientCommands.commands();

        assertEquals("bar", connection.sync().get("foo"));
    }
}
