package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

@Property(name = "spec.name", value = "ByteArrayCodecTest")
@MicronautTest
class ByteArrayCodecTest : AbstractRedisTest() {

    @Inject
    lateinit var codec: RedisCodec<ByteArray, ByteArray>

    // tag::typedConnection[]
    @Inject
    lateinit var connection: StatefulRedisConnection<ByteArray, ByteArray>
    // end::typedConnection[]

    @Test
    fun testByteArrayCodec() {
        assertSame(ByteArrayCodec.INSTANCE, codec)

        val key = "bytes".toByteArray()
        val value = "value".toByteArray()
        val commands = connection.sync()
        commands.set(key, value)

        assertArrayEquals(value, commands.get(key))
    }
}
