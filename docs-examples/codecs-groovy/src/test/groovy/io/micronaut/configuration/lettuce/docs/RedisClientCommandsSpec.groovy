package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

@MicronautTest
class RedisClientCommandsSpec extends AbstractRedisTest {

    @Inject
    RedisClientCommands redisClientCommands

    @Inject
    StatefulRedisConnection<String, String> connection

    void "test commands"() {
        when:
        redisClientCommands.commands()

        then:
        connection.sync().get("foo") == "bar"
    }
}
