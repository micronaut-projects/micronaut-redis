package io.micronaut.configuration.lettuce.cache

import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.jackson.serialize.JacksonObjectSerializer
import spock.lang.Specification

class RedisCacheConfigurationBindingSpec extends Specification {

    void "cache configuration declares serializer setters"() {
        expect:
        setter("setServer", String)
        setter("setKeySerializer", Class)
        setter("setValueSerializer", Class)
    }

    void "default cache configuration binds value serializer"() {
        given:
        ApplicationContext context = ApplicationContext.run(
                'redis.cache.value-serializer': JacksonObjectSerializer.name
        )

        when:
        DefaultRedisCacheConfiguration configuration = context.getBean(DefaultRedisCacheConfiguration)

        then:
        configuration.valueSerializer.orElseThrow() == JacksonObjectSerializer

        cleanup:
        context.close()
    }

    void "named cache configuration binds value serializer"() {
        given:
        ApplicationContext context = ApplicationContext.run(
                'redis.caches.test.enabled': 'true',
                'redis.caches.test.value-serializer': JacksonObjectSerializer.name
        )

        when:
        RedisCacheConfiguration configuration = context.getBean(RedisCacheConfiguration, Qualifiers.byName("test"))

        then:
        configuration.valueSerializer.orElseThrow() == JacksonObjectSerializer

        cleanup:
        context.close()
    }

    private static boolean setter(String methodName, Class<?> parameterType) {
        AbstractRedisCacheConfiguration.getMethod(methodName, parameterType) != null
    }
}
