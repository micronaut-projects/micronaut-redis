package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@Property(name = "spec.name", value = "NamedCodecTest")
@MicronautTest
class NamedCodecTest : AbstractRedisTest() {

    @Inject
    @field:Named("foo")
    lateinit var fooConnection: StatefulRedisConnection<ByteArray, ByteArray>

    @Inject
    @field:Named("bar")
    lateinit var barConnection: StatefulRedisConnection<String, String>

    override fun getProperties(): Map<String, String> {
        val properties = super.getProperties().toMutableMap()
        properties["redis.servers.foo.uri"] = properties.getValue("redis.uri")
        properties["redis.servers.bar.uri"] = properties.getValue("redis.uri")
        return properties
    }

    @Test
    fun testNamedCodecs() {
        val key = "foo-key".toByteArray()
        val value = "foo-value".toByteArray()
        val fooCommands = fooConnection.sync()
        fooCommands.set(key, value)
        val barCommands = barConnection.sync()
        barCommands.set("bar-key", "bar-value")

        assertArrayEquals(value, fooCommands.get(key))
        assertEquals("bar-value", barCommands.get("bar-key"))
    }
}
