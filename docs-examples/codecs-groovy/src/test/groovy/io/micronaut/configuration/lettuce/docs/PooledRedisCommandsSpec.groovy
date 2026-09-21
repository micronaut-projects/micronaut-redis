package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

@Property(name = "redis.pool.enabled", value = "true")
@MicronautTest
class PooledRedisCommandsSpec extends AbstractRedisTest {

    @Inject
    PooledRedisCommands pooledRedisCommands

    @Inject
    StatefulRedisConnection<String, String> connection

    void "test pooled commands"() {
        when:
        pooledRedisCommands.commands()

        then:
        connection.sync().get("first") == "one"
    }
}
