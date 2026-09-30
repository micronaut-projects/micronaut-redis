package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Property(name = "spec.name", value = "NamedConnectionTest")
@MicronautTest
class NamedConnectionTest extends AbstractRedisTest {

    @Inject
    NamedConnectionCommands namedConnectionCommands;

    @Inject
    @Named("foo")
    StatefulRedisConnection<String, String> fooConnection;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>(super.getProperties());
        properties.put("redis.servers.foo.uri", properties.get("redis.uri"));
        return properties;
    }

    @Test
    void testNamedConnection() {
        namedConnectionCommands.commands();

        assertEquals("foo", fooConnection.sync().get("named"));
    }
}
