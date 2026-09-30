package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest
class RedisClientCommandsTest : AbstractRedisTest() {

    @Inject
    lateinit var redisClientCommands: RedisClientCommands

    @Inject
    lateinit var connection: StatefulRedisConnection<String, String>

    @Test
    fun testCommands() {
        redisClientCommands.commands()

        assertEquals("bar", connection.sync().get("foo"))
    }
}
