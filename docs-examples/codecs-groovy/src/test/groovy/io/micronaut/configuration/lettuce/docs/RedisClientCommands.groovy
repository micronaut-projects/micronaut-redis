package io.micronaut.configuration.lettuce.docs

import groovy.transform.CompileStatic
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import jakarta.inject.Inject

@CompileStatic
final class RedisClientCommands {

    // tag::commands[]
    @Inject StatefulRedisConnection<String, String> connection

    void commands() {
        RedisCommands<String, String> commands = connection.sync()
        commands.set("foo", "bar")
        commands.get("foo")
    }
    // end::commands[]
}
