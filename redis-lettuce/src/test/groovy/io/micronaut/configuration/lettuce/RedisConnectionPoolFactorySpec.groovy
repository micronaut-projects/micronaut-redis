package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisConnectionStateListener
import io.lettuce.core.ClientOptions
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.push.PushListener
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.protocol.RedisCommand
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.support.AsyncPool
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.configuration.lettuce.cache.RedisAsyncConnectionPoolFactory
import io.micronaut.configuration.lettuce.cache.RedisConnectionPoolCache
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.ApplicationConfiguration
import jakarta.inject.Singleton
import spock.lang.Specification

import java.net.URI
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class RedisConnectionPoolFactorySpec extends Specification {
    private static final String SPEC_NAME = "redis-connection-pool-factory"

    void "creates a pool of redis connections"() {
        given:
        RedisCodec<String, String> codec = Mock()
        RedisClient redisClient = Stub()
        StatefulRedisConnection<String, String> first = new TestStatefulRedisConnection<>()
        StatefulRedisConnection<String, String> second = new TestStatefulRedisConnection<>()
        RedisConnectionPoolFactory<String, String> factory = new RedisConnectionPoolFactory<>(codec)
        DefaultRedisConfiguration configuration = new DefaultRedisConfiguration()
        configuration.setUri(URI.create("redis://localhost"))
        DefaultRedisConnectionPoolConfiguration poolConfiguration = new DefaultRedisConnectionPoolConfiguration(new ApplicationConfiguration())
        poolConfiguration.maxTotal = 2
        poolConfiguration.maxIdle = 2
        poolConfiguration.minIdle = 0

        redisClient.connect(codec) >>> [first, second]

        when:
        AsyncPool<StatefulRedisConnection<String, String>> pool = factory.redisConnectionPool(redisClient, configuration, poolConfiguration)
        StatefulRedisConnection<String, String> acquiredFirst = pool.acquire().get(5, TimeUnit.SECONDS)
        StatefulRedisConnection<String, String> acquiredSecond = pool.acquire().get(5, TimeUnit.SECONDS)

        then:
        acquiredFirst.is(first)
        acquiredSecond.is(second)
        acquiredFirst.isOpen()
        acquiredSecond.isOpen()

        cleanup:
        if (pool != null) {
            if (acquiredFirst != null) {
                pool.release(acquiredFirst).get(5, TimeUnit.SECONDS)
            }
            if (acquiredSecond != null) {
                pool.release(acquiredSecond).get(5, TimeUnit.SECONDS)
            }
            pool.close()
        }
    }

    void "registers a pool bean for application code"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
            'spec.name'          : SPEC_NAME,
            'redis.uri'          : 'redis://localhost',
            'redis.pool.enabled' : true,
            'redis.pool.min-idle': 0,
            'redis.pool.max-total': 2,
        ])
        AsyncPool<StatefulRedisConnection<String, String>> pool = applicationContext.getBean(AsyncPool)
        StatefulRedisConnection<String, String> acquiredFirst = null
        StatefulRedisConnection<String, String> acquiredSecond = null

        when:
        acquiredFirst = pool.acquire().get(5, TimeUnit.SECONDS)
        acquiredSecond = pool.acquire().get(5, TimeUnit.SECONDS)

        then:
        acquiredFirst instanceof TestStatefulRedisConnection
        acquiredSecond instanceof TestStatefulRedisConnection
        !acquiredFirst.is(acquiredSecond)

        cleanup:
        if (pool != null) {
            if (acquiredFirst != null) {
                pool.release(acquiredFirst).get(5, TimeUnit.SECONDS)
            }
            if (acquiredSecond != null) {
                pool.release(acquiredSecond).get(5, TimeUnit.SECONDS)
            }
            pool.close()
        }
        applicationContext?.stop()
    }

    void "injects byte array pool into cache beans when application pool is also present"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
            'spec.name'                 : SPEC_NAME,
            'redis.uri'                 : 'redis://localhost',
            'redis.pool.enabled'        : true,
            'redis.pool.min-idle'       : 0,
            'redis.pool.max-total'      : 2,
            'redis.caches.test.enabled' : true,
        ])

        when:
        RedisConnectionPoolCache redisCache = applicationContext.getBean(RedisConnectionPoolCache, Qualifiers.byName("test"))
        AsyncPool<StatefulRedisConnection<String, String>> applicationPool = applicationContext.getBean(AsyncPool)
        AsyncPool<StatefulConnection<byte[], byte[]>> cachePool = applicationContext.getBean(
            AsyncPool,
            Qualifiers.byName(RedisAsyncConnectionPoolFactory.CACHE_POOL_BEAN)
        )

        then:
        redisCache != null
        applicationPool != null
        cachePool.is(redisCache.getNativeCache())

        cleanup:
        applicationContext?.stop()
    }

    @Factory
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class TestRedisClientFactory {
        @Singleton
        @Replaces(RedisClient)
        RedisClient redisClient() {
            return new TestRedisClient()
        }
    }

    private static final class TestRedisClient extends RedisClient {
        TestRedisClient() {
            super()
        }

        @Override
        <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec) {
            if (codec instanceof ByteArrayCodec) {
                return (StatefulRedisConnection<K, V>) new ByteArrayTestStatefulRedisConnection()
            }
            return new TestStatefulRedisConnection<>()
        }
    }

    private static class TestStatefulRedisConnection<K, V> implements StatefulRedisConnection<K, V> {
        private boolean open = true
        private Duration timeout = Duration.ofSeconds(60)

        @Override
        boolean isMulti() {
            return false
        }

        @Override
        RedisCommands<K, V> sync() {
            return null
        }

        @Override
        RedisAsyncCommands<K, V> async() {
            return null
        }

        @Override
        RedisReactiveCommands<K, V> reactive() {
            return null
        }

        @Override
        void addListener(PushListener listener) {
        }

        @Override
        void removeListener(PushListener listener) {
        }

        @Override
        void addListener(RedisConnectionStateListener listener) {
        }

        @Override
        void removeListener(RedisConnectionStateListener listener) {
        }

        @Override
        void setTimeout(Duration timeout) {
            this.timeout = timeout
        }

        @Override
        Duration getTimeout() {
            return timeout
        }

        @Override
        <T> RedisCommand<K, V, T> dispatch(RedisCommand<K, V, T> command) {
            return command
        }

        @Override
        Collection<RedisCommand<K, V, ?>> dispatch(Collection<? extends RedisCommand<K, V, ?>> commands) {
            return commands
        }

        @Override
        void close() {
            open = false
        }

        @Override
        CompletableFuture<Void> closeAsync() {
            open = false
            return CompletableFuture.completedFuture(null)
        }

        @Override
        boolean isOpen() {
            return open
        }

        @Override
        ClientOptions getOptions() {
            return null
        }

        @Override
        ClientResources getResources() {
            return null
        }

        @Override
        void setAutoFlushCommands(boolean autoFlush) {
        }

        @Override
        void flushCommands() {
        }

        @Override
        RedisCodec<K, V> getCodec() {
            return null
        }
    }

    private static final class ByteArrayTestStatefulRedisConnection extends TestStatefulRedisConnection<byte[], byte[]> {
    }
}
