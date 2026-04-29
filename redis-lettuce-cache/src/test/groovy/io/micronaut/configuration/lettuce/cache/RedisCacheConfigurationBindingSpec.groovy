package io.micronaut.configuration.lettuce.cache

import io.micronaut.context.ApplicationContext
import io.micronaut.core.serialize.ObjectSerializer
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.jackson.serialize.JacksonObjectSerializer
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

class RedisCacheConfigurationBindingSpec extends Specification {

    void "cache configuration declares serializer setters"() {
        expect:
        setter("setServer", String)
        setter("setKeySerializer", Class)
        setter("setValueSerializer", Class)
        setter("setNamespace", String)
    }

    void "default cache configuration binds value serializer"() {
        given:
        ApplicationContext context = ApplicationContext.run(
                'redis.cache.value-serializer': JacksonObjectSerializer.name,
                'redis.cache.namespace': 'tenant-a'
        )

        when:
        DefaultRedisCacheConfiguration configuration = context.getBean(DefaultRedisCacheConfiguration)

        then:
        configuration.valueSerializer.orElseThrow() == JacksonObjectSerializer
        configuration.namespace.orElseThrow() == 'tenant-a'

        cleanup:
        context.close()
    }

    void "named cache configuration binds value serializer"() {
        given:
        ApplicationContext context = ApplicationContext.run(
                'redis.caches.test.enabled': 'true',
                'redis.caches.test.value-serializer': JacksonObjectSerializer.name,
                'redis.caches.test.namespace': 'tenant-b'
        )

        when:
        RedisCacheConfiguration configuration = context.getBean(RedisCacheConfiguration, Qualifiers.byName("test"))

        then:
        configuration.valueSerializer.orElseThrow() == JacksonObjectSerializer
        configuration.namespace.orElseThrow() == 'tenant-b'

        cleanup:
        context.close()
    }

    void "default cache configuration setter methods update values"() {
        given:
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
        DefaultRedisCacheConfiguration configuration = new DefaultRedisCacheConfiguration(applicationConfiguration)

        when:
        configuration.setServer("default")
        configuration.setNamespace("tenant-a")
        configuration.setKeySerializer((Class<ObjectSerializer>) JacksonObjectSerializer)
        configuration.setValueSerializer((Class<ObjectSerializer>) JacksonObjectSerializer)

        then:
        configuration.server.orElseThrow() == "default"
        configuration.namespace.orElseThrow() == "tenant-a"
        configuration.keySerializer.orElseThrow() == JacksonObjectSerializer
        configuration.valueSerializer.orElseThrow() == JacksonObjectSerializer
    }

    void "named cache configuration setter methods update values"() {
        given:
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
        RedisCacheConfiguration configuration = new RedisCacheConfiguration("test", applicationConfiguration)

        when:
        configuration.setServer("named")
        configuration.setNamespace("tenant-b")
        configuration.setKeySerializer((Class<ObjectSerializer>) JacksonObjectSerializer)
        configuration.setValueSerializer((Class<ObjectSerializer>) JacksonObjectSerializer)

        then:
        configuration.server.orElseThrow() == "named"
        configuration.namespace.orElseThrow() == "tenant-b"
        configuration.keySerializer.orElseThrow() == JacksonObjectSerializer
        configuration.valueSerializer.orElseThrow() == JacksonObjectSerializer
    }

    private static boolean setter(String methodName, Class<?> parameterType) {
        AbstractRedisCacheConfiguration.getMethod(methodName, parameterType) != null
    }
}
