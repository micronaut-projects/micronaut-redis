package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.redis.test.RedisContainerUtils
import io.netty.handler.codec.EncoderException

/**
 * @author Jorge F. Sánchez
 */
class RedisCodecFactorySpec extends RedisSpec {

    void "test StringCodec is used by default"() {
        when:
        final applicationContext = ApplicationContext.run('redis.port': RedisContainerUtils.getRedisPort())
        final codec = applicationContext.getBean(RedisCodec)

        then:
        codec instanceof StringCodec

        cleanup:
        applicationContext.stop()
    }

    void "test custom RedisCodec is used when provided"() {
        when:
        final applicationContext = ApplicationContext.run(
                'spec.name': ByteArrayCodecReplacementFactory.SPEC_NAME,
                'redis.port': RedisContainerUtils.getRedisPort()
        )
        applicationContext.registerSingleton(ByteArrayCodec.class, ByteArrayCodec.INSTANCE)

        then:
        applicationContext.getBean(RedisCodec) == ByteArrayCodec.INSTANCE
        applicationContext.findBean(StringCodec).isEmpty()

        cleanup:
        applicationContext.stop()
    }

    void "test multi redis server codec config"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'spec.name': NamedCodecFactory.SPEC_NAME,
                'redis.servers.foo.uri': RedisContainerUtils.getRedisPort("redis://localhost"),
                'redis.servers.foo.client-name': "foo-client-name",
                'redis.servers.bar.uri': RedisContainerUtils.getRedisPort("redis://localhost")
        )

        when: "get foo connection which accepts bytes (see NamedCodecFactory)"
        StatefulRedisConnection fooConnection = applicationContext.getBean(StatefulRedisConnection, Qualifiers.byName("foo"))

        then:
        final fooKey = "foo".bytes
        final fooValue = "bar".bytes
        RedisCommands<byte[], byte[]> fooCommands = fooConnection.sync()
        fooCommands.set(fooKey, fooValue)
        fooCommands.get(fooKey) == fooValue

        when: 'we send the wrong datatype for the codec'
        fooCommands.set("this", "should fail")

        then:
        thrown(EncoderException)

        when: "get bar connection which accepts Strings (see NamedCodecFactory)"
        StatefulRedisConnection barConnection = applicationContext.getBean(StatefulRedisConnection, Qualifiers.byName("bar"))

        then:
        final barKey = "foo"
        final barValue = "bar"
        RedisCommands<String, String> barCommands = barConnection.sync()
        barCommands.set(barKey, barValue)
        barCommands.get(barKey) == barValue

        when: 'we send the wrong datatypes for the codec'
        barCommands.set("this".length(), "should fail".bytes)

        then:
        thrown(EncoderException)

        cleanup:
        applicationContext.stop()
    }
}
