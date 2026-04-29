package io.micronaut.configuration.lettuce.cache

import io.lettuce.core.KeyScanCursor
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanCursor
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.async.RedisKeyAsyncCommands
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.api.sync.RedisKeyCommands
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.async.AsyncNodeSelection
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands
import io.lettuce.core.cluster.api.sync.NodeSelection
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import io.lettuce.core.cluster.models.partitions.RedisClusterNode
import io.lettuce.core.protocol.AsyncCommand
import io.lettuce.core.protocol.RedisCommand
import io.micronaut.context.BeanLocator
import io.micronaut.core.convert.ConversionService
import io.micronaut.core.type.Argument
import io.micronaut.retry.RetryOperationsFactory
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.function.Supplier

class AbstractRedisCacheSpec extends Specification {

    void "cache-specific read retries override default configuration"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        def cache = new TestRedisCache(stubBeanLocator(scheduler), scheduler, 1, 0, 2, null)
        int attempts = 0

        when:
        String result = cache.executeReadOperation({
            attempts++
            if (attempts < 3) {
                throw new RuntimeException("temporary read failure")
            }
            return "value"
        })

        then:
        result == "value"
        attempts == 3

        cleanup:
        cache.close()
    }

    void "sync insert stops after configured retries"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        def cache = new TestRedisCache(stubBeanLocator(scheduler), scheduler, 0, 1, null, 1)
        int attempts = 0

        when:
        cache.executeInsertOperation({
            attempts++
            throw new RuntimeException("temporary write failure")
        })

        then:
        RuntimeException e = thrown()
        e.message == "temporary write failure"
        attempts == 2

        cleanup:
        cache.close()
    }

    void "async read uses default retry configuration"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        def cache = new TestRedisCache(stubBeanLocator(scheduler), scheduler, 2, 0, null, null)
        int attempts = 0

        when:
        String result = cache.executeReadAsyncOperation({
            attempts++
            if (attempts < 3) {
                return failedFuture("temporary read failure")
            }
            return CompletableFuture.completedFuture("value")
        }).join()

        then:
        result == "value"
        attempts == 3

        cleanup:
        cache.close()
    }

    void "async insert stops after configured retries"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        def cache = new TestRedisCache(stubBeanLocator(scheduler), scheduler, 0, 0, null, 2)
        int attempts = 0

        when:
        cache.executeInsertAsyncOperation({
            attempts++
            return failedFuture("temporary write failure")
        }).join()

        then:
        CompletionException e = thrown()
        e.cause.message == "temporary write failure"
        attempts == 3

        cleanup:
        cache.close()
    }

    void "cluster sync key scan aggregates keys from all upstream nodes"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        def cache = new TestRedisCache(stubBeanLocator(scheduler), scheduler, 0, 0, null, null)
        def connection = Mock(StatefulRedisClusterConnection<byte[], byte[]>)
        def clusterCommands = Mock(RedisAdvancedClusterCommands<byte[], byte[]>)
        def nodeSelection = Mock(NodeSelection<byte[], byte[]>)
        def node1 = Mock(RedisCommands<byte[], byte[]>)
        def node2 = Mock(RedisCommands<byte[], byte[]>)
        def upstreamA = RedisClusterNode.of("upstream-a")
        def upstreamB = RedisClusterNode.of("upstream-b")

        when:
        def keys = cache.collectInvalidateKeys(connection, Mock(RedisKeyCommands<byte[], byte[]>), "test:*".bytes)

        then:
        1 * connection.sync() >> clusterCommands
        1 * clusterCommands.upstream() >> nodeSelection
        1 * nodeSelection.asMap() >> [(upstreamA): node1, (upstreamB): node2]
        1 * node1.scan(_ as ScanArgs) >> finishedKeyScanCursor("test:one".bytes)
        1 * node2.scan(_ as ScanArgs) >> finishedKeyScanCursor("test:two".bytes, "test:three".bytes)
        keys == ["test:one".bytes, "test:two".bytes, "test:three".bytes]

        cleanup:
        cache.close()
    }

    void "cluster async key scan aggregates keys from all upstream nodes"() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        def cache = new TestRedisCache(stubBeanLocator(scheduler), scheduler, 0, 0, null, null)
        def connection = Mock(StatefulRedisClusterConnection<byte[], byte[]>)
        def clusterCommands = Mock(RedisAdvancedClusterAsyncCommands<byte[], byte[]>)
        def nodeSelection = Mock(AsyncNodeSelection<byte[], byte[]>)
        def node1 = Mock(RedisAsyncCommands<byte[], byte[]>)
        def node2 = Mock(RedisAsyncCommands<byte[], byte[]>)
        def upstreamA = RedisClusterNode.of("upstream-a")
        def upstreamB = RedisClusterNode.of("upstream-b")

        when:
        def keys = cache.collectInvalidateKeysAsync(connection, Mock(RedisKeyAsyncCommands<byte[], byte[]>), "test:*".bytes)
            .toCompletableFuture()
            .join()

        then:
        1 * connection.async() >> clusterCommands
        1 * clusterCommands.upstream() >> nodeSelection
        1 * nodeSelection.asMap() >> [(upstreamA): node1, (upstreamB): node2]
        1 * node1.scan(_ as ScanCursor, _ as ScanArgs) >> completedRedisFuture(finishedKeyScanCursor("test:one".bytes))
        1 * node2.scan(_ as ScanCursor, _ as ScanArgs) >> completedRedisFuture(finishedKeyScanCursor("test:two".bytes, "test:three".bytes))
        keys == ["test:one".bytes, "test:two".bytes, "test:three".bytes]

        cleanup:
        cache.close()
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

    private static <T> CompletableFuture<T> failedFuture(String message) {
        CompletableFuture<T> future = new CompletableFuture<>()
        future.completeExceptionally(new RuntimeException(message))
        return future
    }

    private static final class TestRedisCache extends AbstractRedisCache<Object> {
        private final ScheduledExecutorService scheduler

        TestRedisCache(BeanLocator beanLocator, ScheduledExecutorService scheduler, Integer defaultReadRetries, Integer defaultInsertRetries, Integer cacheReadRetries, Integer cacheInsertRetries) {
            super(defaultConfiguration(defaultReadRetries, defaultInsertRetries), configuration(cacheReadRetries, cacheInsertRetries), ConversionService.SHARED, beanLocator)
            this.scheduler = scheduler
        }

        @Override
        String getName() {
            return "test"
        }

        @Override
        Object getNativeCache() {
            return new Object()
        }

        @Override
        def <T> T get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
            throw new UnsupportedOperationException("not used in test")
        }

        @Override
        protected <T> Optional<T> getValue(Argument<T> requiredType, byte[] serializedKey) {
            return Optional.empty()
        }

        @Override
        protected <T> void putValue(byte[] serializedKey, T value) {
        }

        @Override
        void invalidate(Object key) {
        }

        @Override
        void invalidateAll() {
        }

        @Override
        void close() {
            scheduler.shutdownNow()
        }

        <T> T executeReadOperation(Supplier<T> supplier) {
            return executeRead(supplier)
        }

        <T> T executeInsertOperation(Supplier<T> supplier) {
            return executeInsert(supplier)
        }

        <T> CompletableFuture<T> executeReadAsyncOperation(Supplier<CompletionStage<T>> supplier) {
            return executeReadAsync(supplier).toCompletableFuture()
        }

        <T> CompletableFuture<T> executeInsertAsyncOperation(Supplier<CompletionStage<T>> supplier) {
            return executeInsertAsync(supplier).toCompletableFuture()
        }

        List<byte[]> collectInvalidateKeys(StatefulConnection<byte[], byte[]> connection,
                                          RedisKeyCommands<byte[], byte[]> redisKeyCommands,
                                          byte[] pattern) {
            return allKeys(connection, redisKeyCommands, pattern)
        }

        CompletionStage<List<byte[]>> collectInvalidateKeysAsync(StatefulConnection<byte[], byte[]> connection,
                                                                 RedisKeyAsyncCommands<byte[], byte[]> redisKeyCommands,
                                                                 byte[] pattern) {
            return allKeys(connection, redisKeyCommands, pattern)
        }

        private static RedisCacheConfiguration configuration(Integer readRetries, Integer insertRetries) {
            ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
            RedisCacheConfiguration configuration = new RedisCacheConfiguration("test", applicationConfiguration)
            if (readRetries != null) {
                configuration.setReadRetries(readRetries)
            }
            if (insertRetries != null) {
                configuration.setInsertRetries(insertRetries)
            }
            return configuration
        }

        private static DefaultRedisCacheConfiguration defaultConfiguration(Integer readRetries, Integer insertRetries) {
            ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration()
            DefaultRedisCacheConfiguration configuration = new DefaultRedisCacheConfiguration(applicationConfiguration)
            if (readRetries != null) {
                configuration.setReadRetries(readRetries)
            }
            if (insertRetries != null) {
                configuration.setInsertRetries(insertRetries)
            }
            return configuration
        }
    }

    private static KeyScanCursor<byte[]> finishedKeyScanCursor(byte[]... keys) {
        KeyScanCursor<byte[]> cursor = new KeyScanCursor<>()
        cursor.setCursor("0")
        cursor.setFinished(true)
        cursor.getKeys().addAll(keys.toList())
        return cursor
    }

    private AsyncCommand<byte[], byte[], KeyScanCursor<byte[]>> completedRedisFuture(KeyScanCursor<byte[]> cursor) {
        RedisCommand<byte[], byte[], KeyScanCursor<byte[]>> command = Mock()
        AsyncCommand<byte[], byte[], KeyScanCursor<byte[]>> future = new AsyncCommand<>(command)
        future.complete(cursor)
        return future
    }
}
