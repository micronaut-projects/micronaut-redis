package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named

import java.nio.charset.StandardCharsets

@Property(name = "spec.name", value = "NamedCodecTest")
@MicronautTest
class NamedCodecSpec extends AbstractRedisTest {

    @Inject
    @Named("foo")
    StatefulRedisConnection<byte[], byte[]> fooConnection

    @Inject
    @Named("bar")
    StatefulRedisConnection<String, String> barConnection

    @Override
    Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>(super.getProperties())
        properties['redis.servers.foo.uri'] = properties['redis.uri']
        properties['redis.servers.bar.uri'] = properties['redis.uri']
        properties
    }

    void "test named codecs"() {
        given:
        byte[] key = "foo-key".getBytes(StandardCharsets.UTF_8)
        byte[] value = "foo-value".getBytes(StandardCharsets.UTF_8)
        RedisCommands<byte[], byte[]> fooCommands = fooConnection.sync()
        RedisCommands<String, String> barCommands = barConnection.sync()

        when:
        fooCommands.set(key, value)
        barCommands.set("bar-key", "bar-value")

        then:
        fooCommands.get(key) == value
        barCommands.get("bar-key") == "bar-value"
    }
}
