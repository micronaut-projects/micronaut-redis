package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Property(name = "redis.pool.enabled", value = "true")
@MicronautTest
class PooledRedisCommandsTest extends AbstractRedisTest {

    @Inject
    PooledRedisCommands pooledRedisCommands;

    @Inject
    StatefulRedisConnection<String, String> connection;

    @Test
    void testPooledCommands() throws Exception {
        pooledRedisCommands.commands();

        assertEquals("one", connection.sync().get("first"));
    }
}
