package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named

@Property(name = "spec.name", value = "NamedConnectionTest")
@MicronautTest
class NamedConnectionSpec extends AbstractRedisTest {

    @Inject
    NamedConnectionCommands namedConnectionCommands

    @Inject
    @Named("foo")
    StatefulRedisConnection<String, String> fooConnection

    @Override
    Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>(super.getProperties())
        properties['redis.servers.foo.uri'] = properties['redis.uri']
        properties
    }

    void "test named connection"() {
        when:
        namedConnectionCommands.commands()

        then:
        fooConnection.sync().get("named") == "foo"
    }
}
