package io.micronaut.configuration.lettuce.cache

import io.lettuce.core.ReadFrom
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.micronaut.configuration.lettuce.RedisClusterSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.core.type.Argument
import io.micronaut.inject.qualifiers.Qualifiers

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class RedisClusterCacheSpec extends RedisClusterSpec {

    ApplicationContext createApplicationContext() {
        ApplicationContext.run([
                'redis.uris': redisClusterUris,
                'redis.read-from': "replicaPreferred",
                'redis.caches.test.enabled': 'true'
        ])
    }

    void "test read/write object from redis sync cache with read-from"() {
        setup:
        ApplicationContext applicationContext = createApplicationContext()
        RedisClusterClient client = applicationContext.getBean(RedisClusterClient)
        fixPartitions(client)

        when:
        RedisCache redisCache = applicationContext.getBean(RedisCache, Qualifiers.byName("test"))
        StatefulConnection connection = redisCache.getNativeCache()

        then:
        redisCache != null
        connection instanceof StatefulRedisClusterConnection
        ((StatefulRedisClusterConnection) connection).getReadFrom() == ReadFrom.REPLICA_PREFERRED


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

    static class Foo implements Serializable {
        String name
    }
}
