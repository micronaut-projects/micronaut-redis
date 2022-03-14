package io.micronaut.configuration.lettuce.cache

import io.lettuce.core.support.AsyncPool
import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanLocator
import io.micronaut.context.exceptions.ConfigurationException
import io.micronaut.core.convert.DefaultConversionService
import io.micronaut.core.type.Argument
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

import java.nio.charset.Charset

/**
 * @author Kovalov Illia
 */
class RedisPoolCacheSpec extends Specification {

    static ApplicationContext createApplicationContext() {
        ApplicationContext.run(
                'redis.type': 'embedded',
                'redis.caches.test.enabled': 'true',
                'redis.pool.enabled': 'true'
        )
    }

    void "test read/write object from redis sync cache"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext()

        when:
        RedisConnectionPoolCache redisCache = applicationContext.getBean(RedisConnectionPoolCache, Qualifiers.byName("test"))

        then:
        redisCache != null
        redisCache.getNativeCache() instanceof AsyncPool

        when:
        redisCache.put("test", new Foo(name: "test"))
        redisCache.put("two", new Foo(name: "two"))
        redisCache.put("test-list", [new Foo(name: "abc")] as List<Foo>)
        redisCache.put("three", 3)
        redisCache.put("four", "four")
        Foo foo = redisCache.get("test", Foo).get()

        then:
        foo != null
        foo.name == 'test'
        redisCache.async().get("two", Foo.class).get().get().name == "two"
        redisCache.async().get("three", Integer.class).get().get() == 3
        redisCache.async().get("four", String.class).get().get() == "four"
        redisCache.async().get("test-list", Argument.listOf(Foo)).get().get().get(0) instanceof Foo
        redisCache.async().get("test-list", Argument.listOf(Foo)).get().get().get(0).name == "abc"

        when:
        redisCache.invalidate("test")

        then:
        !redisCache.get("test", Foo).isPresent()
        !redisCache.async().get("test", Foo).get().isPresent()
        redisCache.get("two", Foo).isPresent()

        when:
        redisCache.async().put("three", new Foo(name: "three")).get()
        Foo two = redisCache.async().get("two", Foo, {-> new Foo(name: "two")}).get()
        Foo created = redisCache.async().get("new", Foo, {-> new Foo(name: "new")}).get()

        then:
        two != null
        created != null
        created.name == "new"
        redisCache.get("three", Foo).isPresent()
        redisCache.async().get("three", Foo).get().isPresent()
        redisCache.get("new", Foo).isPresent()

        when:
        redisCache.async().invalidate("three").get()

        then:
        !redisCache.async().get("three", Foo).get().isPresent()
        redisCache.get("new", Foo).isPresent()

        when:
        redisCache.invalidateAll()

        then:
        !redisCache.get("test", Foo).isPresent()
        !redisCache.get("two", Foo).isPresent()
        !redisCache.get("new", Foo).isPresent()
        !redisCache.get("four", Foo).isPresent()

        then:
        // invalidate an empty cache should not fail
        redisCache.invalidateAll()

        cleanup:
        applicationContext.stop()
    }

    void "test creating expiration after write policy that is not of type ExpirationAfterWritePolicy"() {
        given:
        ApplicationContext applicationContext = createApplicationContext()

        ApplicationConfiguration appConfig = new ApplicationConfiguration()
        appConfig.setDefaultCharset(Charset.defaultCharset())
        RedisCacheConfiguration cacheConfig = new RedisCacheConfiguration("test3", appConfig)
        cacheConfig.setExpirationAfterWritePolicy("io.micronaut.configuration.lettuce.cache.TimeService")

        when:
        new RedisConnectionPoolCache(
                new DefaultRedisCacheConfiguration(appConfig),
                cacheConfig,
                new DefaultConversionService(),
                applicationContext.getBean(BeanLocator.class),
                applicationContext.getBean(AsyncPool.class)
        )

        then:
        thrown ConfigurationException

        cleanup:
        applicationContext.stop()
    }

    void "test creating expiration after write policy that does not exist"() {
        given:
        ApplicationContext applicationContext = createApplicationContext()

        ApplicationConfiguration appConfig = new ApplicationConfiguration()
        appConfig.setDefaultCharset(Charset.defaultCharset())
        RedisCacheConfiguration cacheConfig = new RedisCacheConfiguration("test3", appConfig)
        cacheConfig.setExpirationAfterWritePolicy("io.micronaut.configuration.lettuce.cache.MissingClass")

        when:
        new RedisConnectionPoolCache(
                new DefaultRedisCacheConfiguration(appConfig),
                cacheConfig,
                new DefaultConversionService(),
                applicationContext.getBean(BeanLocator.class),
                applicationContext.getBean(AsyncPool.class)
        )

        then:
        thrown ConfigurationException

        cleanup:
        applicationContext.stop()
    }

    void "test creating expiration after write policy that exists but has no bean"() {
        given:
        ApplicationContext applicationContext = createApplicationContext()

        ApplicationConfiguration appConfig = new ApplicationConfiguration()
        appConfig.setDefaultCharset(Charset.defaultCharset())
        RedisCacheConfiguration cacheConfig = new RedisCacheConfiguration("test3", appConfig)
        cacheConfig.setExpirationAfterWritePolicy("java.math.BigDecimal")

        when:
        new RedisConnectionPoolCache(
                new DefaultRedisCacheConfiguration(appConfig),
                cacheConfig,
                new DefaultConversionService(),
                applicationContext.getBean(BeanLocator.class),
                applicationContext.getBean(AsyncPool.class)
        )

        then:
        thrown ConfigurationException

        cleanup:
        applicationContext.stop()
    }

    static class Foo implements Serializable {
        String name
    }
}
