package io.micronaut.configuration.lettuce.cache

import groovy.transform.Canonical
import io.lettuce.core.support.AsyncPool
import io.micronaut.configuration.lettuce.AbstractRedisConnectionPoolConfiguration
import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanLocator
import io.micronaut.context.exceptions.ConfigurationException
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.core.convert.DefaultConversionService
import io.micronaut.core.type.Argument
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.redis.test.RedisContainerUtils
import io.micronaut.runtime.ApplicationConfiguration

import java.nio.charset.Charset

/**
 * @author Kovalov Illia
 */
class RedisPoolCacheSpec extends RedisSpec {

    ApplicationContext createApplicationContext(Map options = [:], boolean eagerInit = false) {
        ApplicationContext.builder().properties([
                'redis.port': RedisContainerUtils.getRedisPort(),
                'redis.caches.test.enabled': 'true',
                'redis.pool.enabled': 'true'
        ] + options).environments("test").eagerInitSingletons(eagerInit).start()
    }

    void "can be disabled where initialization is #description"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext('redis.pool.enabled': 'false', eager)

        when:
        applicationContext.getBean(RedisConnectionPoolCache, Qualifiers.byName("test"))

        then:
        thrown NoSuchBeanException

        cleanup:
        applicationContext.stop()

        where:
        eager | description
        true  | 'eager'
        false | 'lazy'
    }

    void "accepts configuration"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext(
                'redis.pool.max-total': 32,
                'redis.pool.max-idle': 12,
                'redis.pool.min-idle': 1
        )

        when:
        applicationContext.getBean(RedisConnectionPoolCache, Qualifiers.byName("test"))

        then:
        noExceptionThrown()

        when:
        def cfg = applicationContext.getBean(AbstractRedisConnectionPoolConfiguration)

        then:
        cfg.maxTotal.get() == 32
        cfg.maxIdle.get() == 12
        cfg.minIdle.get() == 1

        cleanup:
        applicationContext.stop()
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
        redisCache.putIfAbsent("two", new Foo(name: "absent"))

        redisCache.async().putIfAbsent("two", new Foo(name: "async-absent")).get()
        redisCache.async().putIfAbsent("new", new Foo(name: "new")).get()

        redisCache.put("test-list", [new Foo(name: "abc")] as List<Foo>)
        redisCache.put("three", 3)
        redisCache.put("four", "four")
        Foo foo = redisCache.get("test", Foo).get()

        then:
        foo != null
        foo.name == 'test'

        redisCache.async().get("new", Foo.class).get().get().name == "new"
        redisCache.async().get("two", Foo.class).get().get().name == "two"
        redisCache.async().get("three", Integer.class).get().get() == 3
        redisCache.async().get("four", String.class).get().get() == "four"
        redisCache.async().get("test-list", Argument.listOf(Foo)).get().get().get(0) instanceof Foo
        redisCache.async().get("test-list", Argument.listOf(Foo)).get().get().get(0).name == "abc"
        redisCache.async().get("supplier-test", Argument.listOf(Foo), {-> new Foo(name: "cool")}).get() == new Foo(name: "cool")

        when:
        redisCache.invalidate("test")

        then:
        !redisCache.get("test", Foo).isPresent()
        !redisCache.async().get("test", Foo).get().isPresent()
        redisCache.get("two", Foo).isPresent()
        redisCache.get("test", Foo, {-> new Foo(name: "test")}) == new Foo(name: "test")

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
        def result = redisCache.async().invalidateAll().get()

        then:
        result
        !redisCache.get("test", Foo).isPresent()
        !redisCache.get("two", Foo).isPresent()
        !redisCache.get("new", Foo).isPresent()
        !redisCache.get("four", Foo).isPresent()

        then:
        // invalidate an empty cache should not fail
        redisCache.invalidateAll()
        redisCache.async().invalidateAll().get()

        cleanup:
        applicationContext.stop()
    }

    void "check expiration after access"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext(
                'redis.pool.max-total': 32,
                'redis.pool.max-idle': 12,
                'redis.pool.min-idle': 1,
                'redis.caches.test.expire-after-access': '1s',
        )

        when:
        RedisConnectionPoolCache redisCache = applicationContext.getBean(RedisConnectionPoolCache, Qualifiers.byName("test"))

        redisCache.get('a', Argument.of(Foo), {-> new Foo(name: 'a')})
        redisCache.put('b', new Foo(name: 'b'))
        redisCache.async().get('c', Argument.of(Foo), {-> new Foo(name: 'c')}).get()
        redisCache.async().put('d', new Foo(name: 'd')).get()

        then:
        redisCache.get('a', Argument.of(Foo)).get() == new Foo(name: 'a')
        redisCache.get('b', Argument.of(Foo)).get() == new Foo(name: 'b')
        redisCache.get('c', Argument.of(Foo)).get() == new Foo(name: 'c')
        redisCache.get('d', Argument.of(Foo)).get() == new Foo(name: 'd')

        when:
        Thread.sleep(1500)

        then:
        !redisCache.get("a", Argument.of(Foo)).present
        !redisCache.get("b", Argument.of(Foo)).present
        !redisCache.get("c", Argument.of(Foo)).present
        !redisCache.get("d", Argument.of(Foo)).present

        cleanup:
        applicationContext.close()
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

    @Canonical
    static class Foo implements Serializable {
        String name
    }
}
