/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce.cache;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.support.AsyncPool;
import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.SyncCache;
import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Requires;
import org.jspecify.annotations.NonNull;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An implementation of {@link SyncCache} for Lettuce / Redis using connection pooling.
 *
 * @author Graeme Rocher, Kovalov Illia
 * @since 5.3.0
 */
@EachBean(RedisCacheConfiguration.class)
@Requires(classes = SyncCache.class, property = RedisSetting.REDIS_POOL + ".enabled", defaultValue = StringUtils.FALSE, notEquals = StringUtils.FALSE)
public class RedisConnectionPoolCache extends AbstractRedisCache<AsyncPool<StatefulConnection<byte[], byte[]>>> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisConnectionPoolCache.class);
    private final RedisAsyncCache asyncCache;
    private final AsyncPool<StatefulConnection<byte[], byte[]>> asyncPool;

    /**
     * Creates a new redis cache for the given arguments.
     *
     * @param defaultRedisCacheConfiguration The default configuration
     * @param redisCacheConfiguration        The configuration
     * @param conversionService              The conversion service
     * @param beanLocator                    The bean locator used to discover the redis connection from the configuration
     * @param asyncPool                      Redis async pool
     */
    @SuppressWarnings("unchecked")
    public RedisConnectionPoolCache(
            DefaultRedisCacheConfiguration defaultRedisCacheConfiguration,
            RedisCacheConfiguration redisCacheConfiguration,
            ConversionService conversionService,
            BeanLocator beanLocator,
            AsyncPool<StatefulConnection<byte[], byte[]>> asyncPool
    ) {
        super(defaultRedisCacheConfiguration, redisCacheConfiguration, conversionService, beanLocator);
        this.asyncCache = new RedisAsyncCache();
        this.asyncPool = asyncPool;
    }

    @Override
    public String getName() {
        return redisCacheConfiguration.getCacheName();
    }

    @Override
    public AsyncPool<StatefulConnection<byte[], byte[]>> getNativeCache() {
        return asyncPool;
    }

    @Override
    public <T> T get(@NonNull Object key, @NonNull Argument<T> requiredType, @NonNull Supplier<T> supplier) {
        byte[] serializedKey = serializeKey(key);
        byte[] data = executeRead(() -> asyncPool.acquire().thenCompose(connection -> {
            try {
                RedisStringCommands<byte[], byte[]> commands = getRedisStringCommands(connection);
                return CompletableFuture.completedFuture(commands.get(serializedKey));
            } finally {
                asyncPool.release(connection);
            }
        }).join());
        if (data != null) {
            Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
            if (deserialized.isPresent()) {
                return deserialized.get();
            }
        }

        T value = supplier.get();
        putValue(serializedKey, value);
        return value;
    }

    /**
     * Resolve the values for the given keys.
     *
     * @param keys The cache keys
     * @param <K> The key type
     * @return An ordered map containing all requested keys
     */
    @NonNull
    public <K> Map<K, Object> getAll(@NonNull Collection<K> keys) {
        return getAll(keys, Argument.OBJECT_ARGUMENT);
    }

    /**
     * Resolve the values for the given keys.
     *
     * @param keys The cache keys
     * @param requiredType The required type
     * @param <K> The key type
     * @param <T> The value type
     * @return An ordered map containing all requested keys
     */
    @NonNull
    public <K, T> Map<K, T> getAll(@NonNull Collection<K> keys, @NonNull Argument<T> requiredType) {
        return asyncPool.acquire().thenCompose(connection -> {
            try {
                RedisStringCommands<byte[], byte[]> stringCommands = getRedisStringCommands(connection);
                RedisKeyCommands<byte[], byte[]> keyCommands = getRedisKeyCommands(connection);
                return CompletableFuture.completedFuture(getValues(keys, requiredType, stringCommands, keyCommands));
            } finally {
                asyncPool.release(connection);
            }
        }).join();
    }

    /**
     * Cache the specified values in bulk.
     *
     * @param values The values to cache
     */
    public void putAll(@NonNull Map<?, ?> values) {
        asyncPool.acquire().thenAccept(connection -> {
            try {
                RedisStringCommands<byte[], byte[]> stringCommands = getRedisStringCommands(connection);
                RedisKeyCommands<byte[], byte[]> keyCommands = getRedisKeyCommands(connection);
                putValues(values, stringCommands, keyCommands);
            } finally {
                asyncPool.release(connection);
            }
        }).join();
    }

    @Override
    public void invalidate(Object key) {
        byte[] serializedKey = serializeKey(key);
        asyncPool.acquire().thenAccept(connection -> {
            try {
                RedisKeyCommands<byte[], byte[]> commands = getRedisKeyCommands(connection);
                invalidate(Collections.singletonList(serializedKey), commands);
            } finally {
                asyncPool.release(connection);
            }
        }).join();
    }

    /**
     * Invalidate the values for the given keys.
     *
     * @param keys The keys to invalidate
     */
    public void invalidateAllKeys(@NonNull Collection<?> keys) {
        asyncPool.acquire().thenAccept(connection -> {
            try {
                RedisKeyCommands<byte[], byte[]> commands = getRedisKeyCommands(connection);
                invalidateValues(keys, commands);
            } finally {
                asyncPool.release(connection);
            }
        }).join();
    }

    private void invalidate(List<byte[]> key, RedisKeyCommands<byte[], byte[]> commands) {
        commands.del(key.toArray(new byte[0][]));
    }

    @Override
    public void invalidateAll() {
        asyncPool.acquire().thenAccept(connection -> {
            try {
                RedisKeyCommands<byte[], byte[]> commands = getRedisKeyCommands(connection);
                List<byte[]> keys = allKeys(commands, getKeysPattern().getBytes(redisCacheConfiguration.getCharset()));
                if (!keys.isEmpty()) {
                    invalidate(keys, commands);
                }
            } finally {
                asyncPool.release(connection);
            }
        }).join();
    }

    private List<byte[]> allKeys(RedisKeyCommands<byte[], byte[]> commands, byte[] pattern) {
        ScanArgs args = ScanArgs.Builder.limit(invalidateScanCount).match(pattern);
        ScanIterator<byte[]> scanIterator = ScanIterator.scan(commands, args);

        return scanIterator.stream().collect(Collectors.toList());
    }

    @Override
    public AsyncCache<AsyncPool<StatefulConnection<byte[], byte[]>>> async() {
        return asyncCache;
    }

    /**
     * Get the value based on the parameters.
     *
     * @param requiredType  requiredType
     * @param serializedKey serializedKey
     * @param <T>           type of the argument
     * @return value
     */
    @Override
    protected <T> Optional<T> getValue(Argument<T> requiredType, byte[] serializedKey) {
        return executeRead(() -> asyncPool.acquire().thenCompose(connection -> {
            try {
                RedisStringCommands<byte[], byte[]> stringCommands = getRedisStringCommands(connection);
                RedisKeyCommands<byte[], byte[]> keyCommands = getRedisKeyCommands(connection);
                return CompletableFuture.completedFuture(getValue(requiredType, serializedKey, stringCommands, keyCommands));
            } finally {
                asyncPool.release(connection);
            }
        }).join());
    }

    private <T> Optional<T> getValue(
            Argument<T> requiredType,
            byte[] serializedKey,
            RedisStringCommands<byte[], byte[]> stringCommands,
            RedisKeyCommands<byte[], byte[]> keyCommands
    ) {
        byte[] data = stringCommands.get(serializedKey);
        if (expireAfterAccess != null) {
            keyCommands.pexpire(serializedKey, expireAfterAccess);
        }
        if (data != null) {
            return valueSerializer.deserialize(data, requiredType);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Place the value in the cache.
     *
     * @param serializedKey serializedKey
     * @param value         value
     * @param <T>           type of the value
     */
    @Override
    protected <T> void putValue(byte[] serializedKey, T value) {
        Optional<byte[]> serialized = valueSerializer.serialize(value);
        executeInsert(() -> {
            asyncPool.acquire().thenAccept(connection -> {
                try {
                    RedisStringCommands<byte[], byte[]> stringCommands = getRedisStringCommands(connection);
                    RedisKeyCommands<byte[], byte[]> keyCommands = getRedisKeyCommands(connection);
                    putValue(serializedKey, serialized, stringCommands, keyCommands);
                } finally {
                    asyncPool.release(connection);
                }
            }).join();
            return null;
        });
    }

    private void putValue(byte[] serializedKey,
                          Optional<byte[]> value,
                          RedisStringCommands<byte[], byte[]> stringCommands,
                          RedisKeyCommands<byte[], byte[]> keyCommands
    ) {
        putValue(serializedKey,
                value,
                expireAfterWritePolicy,
                stringCommands,
                keyCommands,
                value);
    }

    @PreDestroy
    @Override
    public void close() {
        asyncPool.close();
    }

    /**
     * Redis Async cache implementation.
     */
    protected class RedisAsyncCache implements AsyncCache<AsyncPool<StatefulConnection<byte[], byte[]>>> {

        @Override
        public <T> CompletableFuture<Optional<T>> get(Object key, Argument<T> requiredType) {
            byte[] serializedKey = serializeKey(key);
            return executeReadAsync(() -> asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> commands = getRedisStringAsyncCommands(connection);
                RedisKeyAsyncCommands<byte[], byte[]> keyCommands = getRedisKeyAsyncCommands(connection);

                return commands.get(serializedKey).thenCompose(data -> {
                    if (data != null) {
                        return getWithExpire(requiredType, serializedKey, data, keyCommands);
                    }
                    return CompletableFuture.completedFuture(Optional.<T>empty());
                }).whenComplete((data, ex) -> asyncPool.release(connection));
            })).whenComplete((data, ex) -> {
                if (ex != null) {
                    LOG.error(ex.getMessage(), ex);
                }
            }).toCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
            byte[] serializedKey = serializeKey(key);
            return executeReadAsync(() -> asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> stringCommands = getRedisStringAsyncCommands(connection);
                RedisKeyAsyncCommands<byte[], byte[]> keyCommands = getRedisKeyAsyncCommands(connection);
                return stringCommands.get(serializedKey).thenCompose(data -> {
                    if (data != null) {
                        Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
                        boolean hasValue = deserialized.isPresent();
                        if (expireAfterAccess != null && hasValue) {
                            return keyCommands.expire(serializedKey, expireAfterAccess).thenApply(ignore -> deserialized.get());
                        } else if (hasValue) {
                            return CompletableFuture.completedFuture(deserialized.get());
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                }).whenComplete((data, ex) -> asyncPool.release(connection));
            })).whenComplete((data, ex) -> {
                if (ex != null) {
                    LOG.error(ex.getMessage(), ex);
                }
            }).thenCompose(value -> value != null ? CompletableFuture.completedFuture(value) : putFromSupplier(serializedKey, supplier))
                    .toCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<Optional<T>> putIfAbsent(Object key, T value) {
            byte[] serializedKey = serializeKey(key);
            return executeReadAsync(() -> asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> stringCommands = getRedisStringAsyncCommands(connection);
                RedisKeyAsyncCommands<byte[], byte[]> keyCommands = getRedisKeyAsyncCommands(connection);
                return stringCommands.get(serializedKey).thenCompose(data -> {
                    if (data != null) {
                        return getWithExpire(Argument.of((Class<T>) value.getClass()), serializedKey, data, keyCommands);
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }).whenComplete((data, ex) -> asyncPool.release(connection));
            })).whenComplete((data, ex) -> {
                if (ex != null) {
                    LOG.error(ex.getMessage(), ex);
                }
            }).thenCompose(existing -> {
                if (existing.isPresent()) {
                    return CompletableFuture.completedFuture(existing);
                }
                Optional<byte[]> serialized = valueSerializer.serialize(value);
                if (serialized.isPresent()) {
                    return putWithExpire(serializedKey, serialized.get(), value).thenApply(ignore -> Optional.of(value));
                }
                return CompletableFuture.completedFuture(Optional.<T>empty());
            }).toCompletableFuture();
        }

        @Override
        public CompletableFuture<Boolean> put(Object key, Object value) {
            byte[] serializedKey = serializeKey(key);
            Optional<byte[]> serialized = valueSerializer.serialize(value);
            if (serialized.isPresent()) {
                return putWithExpire(serializedKey, serialized.get(), value).toCompletableFuture();
            }
            return deleteByKeys(serializedKey);
        }

        @Override
        public CompletableFuture<Boolean> invalidate(Object key) {
            return deleteByKeys(serializeKey(key));
        }

        @Override
        public CompletableFuture<Boolean> invalidateAll() {
            return asyncPool.acquire().thenCompose(connection -> {
                RedisKeyAsyncCommands<byte[], byte[]> commands = getRedisKeyAsyncCommands(connection);

                ScanArgs args = ScanArgs.Builder.limit(invalidateScanCount).match(getKeysPattern().getBytes(redisCacheConfiguration.getCharset()));
                return allKeys(commands, ScanCursor.INITIAL, args)
                    .thenCompose(keysToDelete ->
                        deleteByKeys(keysToDelete.toArray(new byte[keysToDelete.size()][]))
                    )
                    .whenComplete((data, ex) -> {
                            asyncPool.release(connection);
                            if (ex != null) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        });
            });
        }

        private CompletableFuture<List<byte[]>> allKeys(RedisKeyAsyncCommands<byte[], byte[]> commands, ScanCursor initialCursor, ScanArgs args) {
            if (initialCursor.isFinished()) {
                return CompletableFuture.completedFuture(new LinkedList<>());
            }
            return (CompletableFuture<List<byte[]>>) commands.scan(initialCursor, args).thenCompose(newCursor -> {
                List<byte[]> keysToDelete = newCursor.getKeys();

                return allKeys(commands, newCursor, args).thenCompose(it -> {
                    it.addAll(keysToDelete);
                    return CompletableFuture.completedFuture(it);
                });
            });
        }

        private CompletableFuture<Boolean> deleteByKeys(byte[]... serializedKey) {
            return asyncPool.acquire().thenCompose(connection -> {
                // Async del cannot handle empty keys
                if (serializedKey.length > 0) {
                    RedisKeyAsyncCommands<byte[], byte[]> commands = getRedisKeyAsyncCommands(connection);
                    return commands.del(serializedKey)
                            .thenApply(keysDeleted -> keysDeleted > 0)
                            .whenComplete((data, ex) -> {
                                asyncPool.release(connection);
                                if (ex != null) {
                                    LOG.error(ex.getMessage(), ex);
                                }
                            });
                } else {
                    return CompletableFuture.completedFuture(true);
                }
            });
        }

        @Override
        public String getName() {
            return RedisConnectionPoolCache.this.getName();
        }

        @Override
        public AsyncPool<StatefulConnection<byte[], byte[]>> getNativeCache() {
            return asyncPool;
        }

        private <T> CompletionStage<Optional<T>> getWithExpire(Argument<T> requiredType, byte[] serializedKey, byte[] data, RedisKeyAsyncCommands<byte[], byte[]> keyCommands) {
            Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
            if (expireAfterAccess != null && deserialized.isPresent()) {
                return keyCommands.expire(serializedKey, expireAfterAccess)
                        .thenApply(ignore -> deserialized);
            }
            return CompletableFuture.completedFuture(deserialized);
        }

        private <T> CompletionStage<T> putFromSupplier(byte[] serializedKey, Supplier<T> supplier) {
            return supply(supplier)
                    .thenCompose(value -> {
                        Optional<byte[]> serialized = valueSerializer.serialize(value);
                        if (serialized.isPresent()) {
                            return putWithExpire(serializedKey, serialized.get(), value).thenApply(ignore -> value);
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        }

        private <T> CompletionStage<T> supply(Supplier<T> supplier) {
            CompletableFuture<T> completableFuture = new CompletableFuture<>();
            try {
                completableFuture.complete(supplier.get());
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
            return completableFuture;
        }

        private CompletionStage<Boolean> putWithExpire(byte[] serializedKey, byte[] serialized, Object value) {
            return executeInsertAsync(() -> asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> commands = getRedisStringAsyncCommands(connection);
                if (expireAfterWritePolicy != null) {
                    return commands.psetex(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value), serialized)
                            .whenComplete((result, ex) -> asyncPool.release(connection))
                            .thenApply(isOK());
                } else {
                    return commands.set(serializedKey, serialized)
                            .whenComplete((result, ex) -> asyncPool.release(connection))
                            .thenApply(isOK());
                }
            })).whenComplete((result, ex) -> {
                if (ex != null) {
                    LOG.error(ex.getMessage(), ex);
                }
            });
        }

        private Function<String, Boolean> isOK() {
            return "OK"::equals;
        }
    }
}
