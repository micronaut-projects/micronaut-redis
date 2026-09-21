package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@Property(name = "redis.pool.enabled", value = "true")
@MicronautTest
class PooledRedisCommandsTest : AbstractRedisTest() {

    @Inject
    lateinit var pooledRedisCommands: PooledRedisCommands

    @Inject
    lateinit var connection: StatefulRedisConnection<String, String>

    @Test
    fun testPooledCommands() {
        pooledRedisCommands.commands()

        assertEquals("one", connection.sync().get("first"))
    }
}
