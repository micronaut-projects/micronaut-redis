package io.micronaut.configuration.lettuce.cache

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.protocol.AsyncCommand
import io.lettuce.core.protocol.RedisCommand
import io.lettuce.core.support.AsyncPool
import io.micronaut.context.BeanLocator
import io.micronaut.core.convert.ConversionService
import io.micronaut.core.serialize.JdkSerializer
import io.micronaut.core.type.Argument
import io.micronaut.retry.RetryOperationsFactory
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

class RedisCacheRetrySpec extends Specification {

    void "redis cache retries failed sync reads before invoking the supplier"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        byte[] cachedValue = serialize("cached")
        RedisCommands<byte[], byte[]> syncCommands = Mock()
        RedisAsyncCommands<byte[], byte[]> asyncCommands = Mock()
        StatefulRedisConnection<byte[], byte[]> connection = Stub() {
            sync() >> syncCommands
            async() >> asyncCommands
        }
        RedisCache cache = new RedisCache(
                defaultConfiguration(),
                configuration("test", 2, null),
                ConversionService.SHARED,
                beanLocator(connection, scheduler)
        )
        int readAttempts = 0
        int supplierCalls = 0

        syncCommands.get(_ as byte[]) >> {
            readAttempts++
            if (readAttempts < 3) {
                throw new RuntimeException("connection reset")
            }
            cachedValue
        }

        when:
        String result = cache.get("key", Argument.of(String), {
            supplierCalls++
            return "fallback"
        })

        then:
        result == "cached"
        readAttempts == 3
        supplierCalls == 0
        0 * syncCommands.set(_ as byte[], _ as byte[])

        cleanup:
        scheduler.shutdownNow()
    }

    void "redis cache retries failed sync inserts without recomputing the supplier value"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        RedisCommands<byte[], byte[]> syncCommands = Mock()
        RedisAsyncCommands<byte[], byte[]> asyncCommands = Mock()
        StatefulRedisConnection<byte[], byte[]> connection = Stub() {
            sync() >> syncCommands
            async() >> asyncCommands
        }
        RedisCache cache = new RedisCache(
                defaultConfiguration(),
                configuration("test", null, 2),
                ConversionService.SHARED,
                beanLocator(connection, scheduler)
        )
        int insertAttempts = 0
        int supplierCalls = 0

        syncCommands.get(_ as byte[]) >> null
        syncCommands.set(_ as byte[], _ as byte[]) >> {
            insertAttempts++
            if (insertAttempts < 3) {
                throw new RuntimeException("connection reset")
            }
            "OK"
        }

        when:
        String result = cache.get("key", Argument.of(String), {
            supplierCalls++
            return "computed"
        })

        then:
        result == "computed"
        insertAttempts == 3
        supplierCalls == 1

        cleanup:
        scheduler.shutdownNow()
    }

    void "pooled redis cache retries failed async reads before invoking the supplier"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        byte[] cachedValue = serialize("cached")
        RedisCommands<byte[], byte[]> syncCommands = Mock()
        RedisAsyncCommands<byte[], byte[]> asyncCommands = Mock()
        StatefulRedisConnection<byte[], byte[]> connection = Stub() {
            sync() >> syncCommands
            async() >> asyncCommands
        }
        AsyncPool<StatefulRedisConnection<byte[], byte[]>> asyncPool = Mock()
        asyncPool.acquire() >> { CompletableFuture.completedFuture(connection) }
        asyncPool.release(connection) >> { CompletableFuture.completedFuture(null) }
        RedisConnectionPoolCache cache = new RedisConnectionPoolCache(
                defaultConfiguration(),
                configuration("test", 2, null),
                ConversionService.SHARED,
                stubBeanLocator(scheduler),
                asyncPool
        )
        int readAttempts = 0
        int supplierCalls = 0

        asyncCommands.get(_ as byte[]) >> {
            readAttempts++
            if (readAttempts < 3) {
                return failedRedisFuture("connection reset")
            }
            return completedRedisFuture(cachedValue)
        }

        when:
        String result = cache.async().get("key", Argument.of(String), {
            supplierCalls++
            return "fallback"
        }).join()

        then:
        result == "cached"
        readAttempts == 3
        supplierCalls == 0
        3 * asyncPool.release(connection)

        cleanup:
        scheduler.shutdownNow()
    }

    void "pooled redis cache retries failed async inserts without recomputing the supplier value"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        RedisCommands<byte[], byte[]> syncCommands = Mock()
        RedisAsyncCommands<byte[], byte[]> asyncCommands = Mock()
        StatefulRedisConnection<byte[], byte[]> connection = Stub() {
            sync() >> syncCommands
            async() >> asyncCommands
        }
        AsyncPool<StatefulRedisConnection<byte[], byte[]>> asyncPool = Mock()
        asyncPool.acquire() >> { CompletableFuture.completedFuture(connection) }
        asyncPool.release(connection) >> { CompletableFuture.completedFuture(null) }
        RedisConnectionPoolCache cache = new RedisConnectionPoolCache(
                defaultConfiguration(),
                configuration("test", null, 2),
                ConversionService.SHARED,
                stubBeanLocator(scheduler),
                asyncPool
        )
        int insertAttempts = 0
        int supplierCalls = 0

        asyncCommands.get(_ as byte[]) >> completedRedisFuture(null)
        asyncCommands.set(_ as byte[], _ as byte[]) >> {
            insertAttempts++
            if (insertAttempts < 3) {
                return failedRedisFuture("connection reset")
            }
            return completedRedisFuture("OK")
        }

        when:
        String result = cache.async().get("key", Argument.of(String), {
            supplierCalls++
            return "computed"
        }).join()

        then:
        result == "computed"
        insertAttempts == 3
        supplierCalls == 1
        4 * asyncPool.release(connection)

        cleanup:
        scheduler.shutdownNow()
    }

    private BeanLocator beanLocator(StatefulRedisConnection<byte[], byte[]> connection, ScheduledExecutorService scheduler) {
        RedisClient redisClient = Stub() {
            connect(_) >> connection
        }
        RetryOperationsFactory retryOperationsFactory = RetryOperationsFactory.create(scheduler)
        Stub(BeanLocator) {
            findBean(RedisClient) >> Optional.of(redisClient)
            findBean(_ as Class) >> { Class type ->
                if (type == RedisClient) {
                    return Optional.of(redisClient)
                }
                return Optional.empty()
            }
            findOrInstantiateBean(_ as Class) >> { Class type ->
                if (type == RetryOperationsFactory) {
                    return Optional.of(retryOperationsFactory)
                }
                return Optional.empty()
            }
        }
    }

    private BeanLocator stubBeanLocator(ScheduledExecutorService scheduler) {
        RetryOperationsFactory retryOperationsFactory = RetryOperationsFactory.create(scheduler)
        Stub(BeanLocator) {
            findOrInstantiateBean(_ as Class) >> { Class type ->
                if (type == RetryOperationsFactory) {
                    return Optional.of(retryOperationsFactory)
                }
                return Optional.empty()
            }
        }
    }

    private static DefaultRedisCacheConfiguration defaultConfiguration() {
        return new DefaultRedisCacheConfiguration(new ApplicationConfiguration())
    }

    private static RedisCacheConfiguration configuration(String name, Integer readRetries, Integer insertRetries) {
        RedisCacheConfiguration configuration = new RedisCacheConfiguration(name, new ApplicationConfiguration())
        if (readRetries != null) {
            configuration.setReadRetries(readRetries)
        }
        if (insertRetries != null) {
            configuration.setInsertRetries(insertRetries)
        }
        return configuration
    }

    private static byte[] serialize(String value) {
        return new JdkSerializer(ConversionService.SHARED).serialize(value).get()
    }

    private static <T> CompletableFuture<T> failedFuture(String message) {
        CompletableFuture<T> future = new CompletableFuture<>()
        future.completeExceptionally(new RuntimeException(message))
        return future
    }

    private <T> RedisFuture<T> completedRedisFuture(T value) {
        AsyncCommand<T, T, T> command = new AsyncCommand<>(Mock(RedisCommand))
        command.complete(value)
        return command
    }

    private <T> RedisFuture<T> failedRedisFuture(String message) {
        AsyncCommand<T, T, T> command = new AsyncCommand<>(Mock(RedisCommand))
        command.completeExceptionally(new RuntimeException(message))
        return command
    }
}
