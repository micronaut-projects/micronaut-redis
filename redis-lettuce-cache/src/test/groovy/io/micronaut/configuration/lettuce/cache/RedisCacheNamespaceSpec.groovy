package io.micronaut.configuration.lettuce.cache

import io.micronaut.context.BeanLocator
import io.micronaut.context.ApplicationContext
import io.micronaut.core.convert.ConversionService
import io.micronaut.core.type.Argument
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

import java.util.function.Supplier

class RedisCacheNamespaceSpec extends Specification {

    void "default namespace prefixes serialized keys and scan pattern"() {
        given:
        ApplicationContext beanLocator = ApplicationContext.run()
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
        DefaultRedisCacheConfiguration defaultConfiguration = new DefaultRedisCacheConfiguration(applicationConfiguration)
        RedisCacheConfiguration cacheConfiguration = new RedisCacheConfiguration("test", applicationConfiguration)
        defaultConfiguration.setNamespace("tenant-a")
        TestRedisCache cache = new TestRedisCache(defaultConfiguration, cacheConfiguration, beanLocator)

        expect:
        cache.serializeKeyToString("alpha") == "tenant-a:test:alpha"
        cache.keysPattern() == "tenant-a:test:*"

        cleanup:
        beanLocator.close()
    }

    void "named cache namespace overrides default namespace"() {
        given:
        ApplicationContext beanLocator = ApplicationContext.run()
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
        DefaultRedisCacheConfiguration defaultConfiguration = new DefaultRedisCacheConfiguration(applicationConfiguration)
        RedisCacheConfiguration cacheConfiguration = new RedisCacheConfiguration("test", applicationConfiguration)
        defaultConfiguration.setNamespace("tenant-a")
        cacheConfiguration.setNamespace("tenant-b")
        TestRedisCache cache = new TestRedisCache(defaultConfiguration, cacheConfiguration, beanLocator)

        expect:
        cache.serializeKeyToString("alpha") == "tenant-b:test:alpha"
        cache.keysPattern() == "tenant-b:test:*"

        cleanup:
        beanLocator.close()
    }

    void "existing key format is preserved when no namespace is configured"() {
        given:
        ApplicationContext beanLocator = ApplicationContext.run()
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
        TestRedisCache cache = new TestRedisCache(
                new DefaultRedisCacheConfiguration(applicationConfiguration),
                new RedisCacheConfiguration("test", applicationConfiguration),
                beanLocator
        )

        expect:
        cache.serializeKeyToString("alpha") == "test:alpha"
        cache.keysPattern() == "test:*"

        cleanup:
        beanLocator.close()
    }

    void "namespace ending with separator is not duplicated"() {
        given:
        ApplicationContext beanLocator = ApplicationContext.run()
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
        DefaultRedisCacheConfiguration defaultConfiguration = new DefaultRedisCacheConfiguration(applicationConfiguration)
        RedisCacheConfiguration cacheConfiguration = new RedisCacheConfiguration("test", applicationConfiguration)
        defaultConfiguration.setNamespace("tenant-a:")
        TestRedisCache cache = new TestRedisCache(defaultConfiguration, cacheConfiguration, beanLocator)

        expect:
        cache.serializeKeyToString("alpha") == "tenant-a:test:alpha"
        cache.keysPattern() == "tenant-a:test:*"

        cleanup:
        beanLocator.close()
    }

    private static final class TestRedisCache extends AbstractRedisCache<Object> {

        TestRedisCache(DefaultRedisCacheConfiguration defaultConfiguration, RedisCacheConfiguration cacheConfiguration, BeanLocator beanLocator) {
            super(defaultConfiguration, cacheConfiguration, ConversionService.SHARED, beanLocator)
        }

        @Override
        String getName() {
            return redisCacheConfiguration.cacheName
        }

        @Override
        Object getNativeCache() {
            return null
        }

        @Override
        <T> T get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
            return supplier.get()
        }

        @Override
        void invalidate(Object key) {
        }

        @Override
        void invalidateAll() {
        }

        @Override
        void close() {
        }

        @Override
        protected <T> Optional<T> getValue(Argument<T> requiredType, byte[] serializedKey) {
            return Optional.empty()
        }

        @Override
        protected <T> void putValue(byte[] serializedKey, T value) {
        }

        String serializeKeyToString(Object key) {
            return new String(serializeKey(key), redisCacheConfiguration.charset)
        }

        String keysPattern() {
            return getKeysPattern()
        }
    }
}
