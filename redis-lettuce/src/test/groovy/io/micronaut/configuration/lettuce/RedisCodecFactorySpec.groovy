package io.micronaut.configuration.lettuce


import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.ApplicationContext
import io.micronaut.redis.test.RedisContainerUtils

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
        final applicationContext = ApplicationContext.run('redis.port': RedisContainerUtils.getRedisPort())
        applicationContext.registerSingleton(ByteArrayCodec.class, ByteArrayCodec.INSTANCE)

        then:
        applicationContext.getBean(RedisCodec) == ByteArrayCodec.INSTANCE
        applicationContext.findBean(StringCodec).isEmpty()

        cleanup:
        applicationContext.stop()
    }
}
