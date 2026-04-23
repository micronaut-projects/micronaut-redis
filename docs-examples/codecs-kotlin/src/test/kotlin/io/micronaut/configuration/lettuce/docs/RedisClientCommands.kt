package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import jakarta.inject.Inject

class RedisClientCommands {

    // tag::commands[]
    @Inject
    lateinit var connection: StatefulRedisConnection<String, String>

    fun commands() {
        val commands: RedisCommands<String, String> = connection.sync()
        commands.set("foo", "bar")
        commands.get("foo")
    }
    // end::commands[]
}
