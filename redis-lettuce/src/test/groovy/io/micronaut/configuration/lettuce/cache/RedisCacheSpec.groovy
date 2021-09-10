/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce.cache

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisStringAsyncCommands
import io.lettuce.core.protocol.AsyncCommand
import io.lettuce.core.protocol.RedisCommand
import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanLocator
import io.micronaut.context.exceptions.ConfigurationException
import io.micronaut.core.convert.DefaultConversionService
import io.micronaut.core.type.Argument
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Requires
import spock.lang.Specification

import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.nio.charset.Charset
import java.util.concurrent.ExecutionException

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class RedisCacheSpec extends Specification {

    ApplicationContext createApplicationContext() {
        ApplicationContext.run(
                'redis.type': 'embedded',
                'redis.caches.test.enabled': 'true'
        )
    }

    void "test read/write object from redis sync cache"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext()

        when:
        RedisCache redisCache = applicationContext.getBean(RedisCache, Qualifiers.byName("test"))

        then:
        redisCache != null
        redisCache.getNativeCache() instanceof StatefulRedisConnection

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
        new RedisCache(
                new DefaultRedisCacheConfiguration(appConfig),
                cacheConfig,
                new DefaultConversionService(),
                applicationContext.getBean(BeanLocator.class)
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
        new RedisCache(
                new DefaultRedisCacheConfiguration(appConfig),
                cacheConfig,
                new DefaultConversionService(),
                applicationContext.getBean(BeanLocator.class)
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
        new RedisCache(
                new DefaultRedisCacheConfiguration(appConfig),
                cacheConfig,
                new DefaultConversionService(),
                applicationContext.getBean(BeanLocator.class)
        )

        then:
        thrown ConfigurationException

        cleanup:
        applicationContext.stop()
    }

    @Requires({jvm.javaVersion.startsWith("1.8")}) // Because of the reflection run only on Java 8
    void "test exceptions"() {
        setup:
            ApplicationContext applicationContext = createApplicationContext()

            def redisStringAsyncCommands = Mock(RedisStringAsyncCommands.class)
            RedisCache redisCache = applicationContext.getBean(RedisCache, Qualifiers.byName("test"))

            Field field = RedisCache.getDeclaredField("redisStringAsyncCommands")
            field.setAccessible(true)

            Field modifiersField = Field.class.getDeclaredField("modifiers")
            modifiersField.setAccessible(true)
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL)

            field.set(redisCache, redisStringAsyncCommands)

            def result = new AsyncCommand(Mock(RedisCommand))
            redisStringAsyncCommands.get(_) >> result
            result.completeExceptionally(new RuntimeException("XYZ"))
        when:
            def op = redisCache.async().get("two", Foo.class)
        then:
            op.isCompletedExceptionally()
        when:
            op.get()
        then:
            def e = thrown(ExecutionException)
            e.cause.message == "XYZ"

        cleanup:
            applicationContext.stop()
    }

    void "test read/write objects in bulk from redis sync cache"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext()

        when:
        RedisCache redisCache = applicationContext.getBean(RedisCache, Qualifiers.byName("test"))

        then:
        redisCache != null
        redisCache.getNativeCache() instanceof StatefulRedisConnection

        when:
        redisCache.put("deleteme", "should get deleted")
        redisCache.put([
            "test": new Foo(name: "test"),
            "two": new Foo(name: "two"),
            "test-list": [new Foo(name: "abc")] as List<Foo>,
            "three": 3,
            "four": "four",
            "deleteme": null,
        ])
        Map<String, Object> result = redisCache.get(["test", "two", "three", "four", "test-list", "deleteme"])

        then:
        result.get("test").name == "test"
        result.get("two").name == "two"
        result.get("three") == 3
        result.get("four") == "four"
        result.get("test-list").get(0) instanceof Foo
        result.get("test-list").get(0).name == "abc"
        result.get("deleteme") == null
        !redisCache.get("deleteme", Object).isPresent()

        when:
        redisCache.invalidate(["test", "two", "three", "four"])

        then:
        !redisCache.get("test", Foo).isPresent()
        !redisCache.get("two", Foo).isPresent()
        !redisCache.get("three", Integer).isPresent()
        !redisCache.get("four", String).isPresent()
        redisCache.get("test-list", List<Foo>).isPresent()
        !redisCache.get("deleteme", Object).isPresent()

        cleanup:
        applicationContext.stop()
    }

    static class Foo implements Serializable {
        String name
    }
}
