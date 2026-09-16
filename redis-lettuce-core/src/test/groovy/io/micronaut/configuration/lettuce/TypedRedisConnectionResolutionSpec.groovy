package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.type.Argument
import io.micronaut.redis.test.RedisContainerUtils
import jakarta.inject.Inject
import jakarta.inject.Singleton
import spock.lang.AutoCleanup
import spock.lang.Shared

import java.nio.charset.StandardCharsets

/**
 * Verifies that an unqualified {@code RedisCodec<byte[], byte[]>} bean produces connection beans that
 * resolve for the concrete type arguments instead of the generic {@code @Primary} default connection.
 */
class TypedRedisConnectionResolutionSpec extends RedisSpec {

    @Shared
    @AutoCleanup
    ApplicationContext applicationContext = ApplicationContext.run(
            'spec.name': ByteArrayCodecFactory.SPEC_NAME,
            'redis.port': RedisContainerUtils.getRedisPort()
    )

    void "typed connection looked up by type arguments uses the byte array codec"() {
        given:
        byte[] key = "typed-key".getBytes(StandardCharsets.UTF_8)
        byte[] value = "typed-value".getBytes(StandardCharsets.UTF_8)

        when:
        StatefulRedisConnection<byte[], byte[]> connection = applicationContext.getBean(Argument.of(StatefulRedisConnection, byte[].class, byte[].class))
        RedisCommands<byte[], byte[]> commands = connection.sync()
        commands.set(key, value)

        then:
        commands.get(key) == value
        applicationContext.getBean(Argument.of(StatefulRedisConnection, byte[].class, byte[].class)).is(connection)
    }

    void "typed connection injected by type arguments uses the byte array codec"() {
        given:
        byte[] key = "injected-key".getBytes(StandardCharsets.UTF_8)
        byte[] value = "injected-value".getBytes(StandardCharsets.UTF_8)

        when:
        ByteArrayConnectionHolder holder = applicationContext.getBean(ByteArrayConnectionHolder)
        holder.connection.sync().set(key, value)

        then:
        holder.connection.sync().get(key) == value
        holder.pubSubConnection.sync().get(key) == value
        holder.connection.is(applicationContext.getBean(Argument.of(StatefulRedisConnection, byte[].class, byte[].class)))
        holder.pubSubConnection.is(applicationContext.getBean(Argument.of(StatefulRedisPubSubConnection, byte[].class, byte[].class)))
    }

    void "the default connection still serves String and raw requests"() {
        when:
        StatefulRedisConnection<String, String> stringConnection = applicationContext.getBean(Argument.of(StatefulRedisConnection, String, String))
        stringConnection.sync().set("string-key", "string-value")

        then:
        stringConnection.sync().get("string-key") == "string-value"
        applicationContext.getBean(StatefulRedisConnection).is(stringConnection)
        applicationContext.getBean(StatefulRedisPubSubConnection).sync().get("string-key") == "string-value"
        applicationContext.getBean(RedisCodec) instanceof StringCodec
        applicationContext.getBean(Argument.of(RedisCodec, byte[].class, byte[].class)).is(ByteArrayCodec.INSTANCE)
        !applicationContext.getBean(Argument.of(StatefulRedisConnection, byte[].class, byte[].class)).is(stringConnection)
    }

    @Requires(property = "spec.name", value = ByteArrayCodecFactory.SPEC_NAME)
    @Singleton
    static class ByteArrayConnectionHolder {

        @Inject
        StatefulRedisConnection<byte[], byte[]> connection

        @Inject
        StatefulRedisPubSubConnection<byte[], byte[]> pubSubConnection
    }
}
