package io.micronaut.configuration.lettuce.cache

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.support.AsyncPool
import io.micronaut.configuration.lettuce.DefaultRedisConfiguration
import io.micronaut.configuration.lettuce.DefaultRedisConnectionPoolConfiguration
import io.micronaut.context.BeanLocator
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

import java.net.URI
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class RedisAsyncConnectionPoolFactorySpec extends Specification {

    void "creates standalone cache connections from redis clients"() {
        given:
        BeanLocator beanLocator = Mock()
        RedisClient redisClient = Mock()
        StatefulRedisConnection<byte[], byte[]> connection = mockPoolableConnection(StatefulRedisConnection)
        RedisAsyncConnectionPoolFactory factory = new RedisAsyncConnectionPoolFactory()
        DefaultRedisCacheConfiguration cacheConfiguration = new DefaultRedisCacheConfiguration(new ApplicationConfiguration())
        DefaultRedisConfiguration redisConfiguration = new DefaultRedisConfiguration()
        redisConfiguration.setUri(URI.create("redis://localhost:6379"))
        DefaultRedisConnectionPoolConfiguration poolConfiguration = poolConfiguration()

        StatefulConnection<byte[], byte[]> acquired = null
        AsyncPool<StatefulConnection<byte[], byte[]>> pool = null

        when:
        pool = factory.getAsyncPool(cacheConfiguration, redisConfiguration, beanLocator, poolConfiguration)
        acquired = pool.acquire().get(5, TimeUnit.SECONDS)

        then:
        acquired != null
        1 * beanLocator.findBean(RedisClusterClient) >> Optional.empty()
        1 * beanLocator.findBean(RedisClient) >> Optional.of(redisClient)
        1 * redisClient.connect(_) >> connection

        cleanup:
        releaseAndClose(pool, acquired)
    }

    void "creates cluster cache connections and applies read from"() {
        given:
        BeanLocator beanLocator = Mock()
        RedisClusterClient redisClient = Mock()
        StatefulRedisClusterConnection<byte[], byte[]> connection = mockPoolableConnection(StatefulRedisClusterConnection)
        RedisAsyncConnectionPoolFactory factory = new RedisAsyncConnectionPoolFactory()
        DefaultRedisCacheConfiguration cacheConfiguration = new DefaultRedisCacheConfiguration(new ApplicationConfiguration())
        DefaultRedisConfiguration redisConfiguration = new DefaultRedisConfiguration()
        redisConfiguration.setReadFrom("MASTER")
        DefaultRedisConnectionPoolConfiguration poolConfiguration = poolConfiguration()

        StatefulConnection<byte[], byte[]> acquired = null
        AsyncPool<StatefulConnection<byte[], byte[]>> pool = null

        when:
        pool = factory.getAsyncPool(cacheConfiguration, redisConfiguration, beanLocator, poolConfiguration)
        acquired = pool.acquire().get(5, TimeUnit.SECONDS)

        then:
        acquired != null
        1 * beanLocator.findBean(RedisClusterClient) >> Optional.of(redisClient)
        1 * redisClient.connect(_) >> connection
        1 * connection.setReadFrom(_)

        cleanup:
        releaseAndClose(pool, acquired)
    }

    private DefaultRedisConnectionPoolConfiguration poolConfiguration() {
        DefaultRedisConnectionPoolConfiguration configuration = new DefaultRedisConnectionPoolConfiguration(new ApplicationConfiguration())
        configuration.maxTotal = 1
        configuration.maxIdle = 1
        configuration.minIdle = 0
        configuration
    }

    private <T extends StatefulConnection<byte[], byte[]>> T mockPoolableConnection(Class<T> type) {
        Mock(type) {
            isOpen() >> true
            closeAsync() >> CompletableFuture.completedFuture(null)
        }
    }

    private static void releaseAndClose(AsyncPool<StatefulConnection<byte[], byte[]>> pool, StatefulConnection<byte[], byte[]> connection) {
        if (pool != null) {
            if (connection != null) {
                pool.release(connection).toCompletableFuture().get(5, TimeUnit.SECONDS)
            }
            pool.close()
        }
    }
}
