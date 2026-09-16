package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@Property(name = "spec.name", value = "NamedConnectionTest")
@MicronautTest
class NamedConnectionTest : AbstractRedisTest() {

    @Inject
    lateinit var namedConnectionCommands: NamedConnectionCommands

    @Inject
    @field:Named("foo")
    lateinit var fooConnection: StatefulRedisConnection<String, String>

    override fun getProperties(): Map<String, String> {
        val properties = super.getProperties().toMutableMap()
        properties["redis.servers.foo.uri"] = properties.getValue("redis.uri")
        return properties
    }

    @Test
    fun testNamedConnection() {
        namedConnectionCommands.commands()

        assertEquals("foo", fooConnection.sync().get("named"))
    }
}
