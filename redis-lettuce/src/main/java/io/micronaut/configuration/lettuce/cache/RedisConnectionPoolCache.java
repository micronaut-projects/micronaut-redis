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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.SyncCache;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An implementation of {@link SyncCache} for Lettuce / Redis using connection pooling.
 *
 * @author Kovalov Illia
 * @since 1.0
 */
@EachBean(RedisCacheConfiguration.class)
@Requires(classes = SyncCache.class, property = RedisSetting.REDIS_POOL)
public class RedisConnectionPoolCache extends AbstractRedisCache<AsyncPool<StatefulConnection<byte[], byte[]>>> {
    private final RedisAsyncCache asyncCache;
    private final AsyncPool<StatefulConnection<byte[], byte[]>> asyncPool;

    /**
     * Creates a new redis cache for the given arguments.
     *
     * @param defaultRedisCacheConfiguration The default configuration
     * @param redisCacheConfiguration        The configuration
     * @param conversionService              The conversion service
     * @param beanLocator                    The bean locator used to discover the redis connection from the configuration
     */
    @SuppressWarnings("unchecked")
    public RedisConnectionPoolCache(
            DefaultRedisCacheConfiguration defaultRedisCacheConfiguration,
            RedisCacheConfiguration redisCacheConfiguration,
            ConversionService<?> conversionService,
            BeanLocator beanLocator
    ) {
        super(defaultRedisCacheConfiguration, redisCacheConfiguration, conversionService, beanLocator);

        Optional<String> server = Optional.ofNullable(
                redisCacheConfiguration
                        .getServer()
                        .orElse(defaultRedisCacheConfiguration.getServer().orElse(null))
        );

        StatefulConnection<Byte[], Byte[]> connection = RedisConnectionUtil.findRedisConnection(beanLocator, server, "No Redis server configured to allow caching");
        this.asyncCache = new RedisAsyncCache();
        AbstractRedisClient client = RedisConnectionUtil.findClient(beanLocator, server, "No Redis server configured to allow caching");
        BoundedPoolConfig asyncConfig = BoundedPoolConfig.builder().minIdle(4).maxIdle(16).maxTotal(32).build();
        CompletionStage<AsyncPool<StatefulConnection<byte[], byte[]>>> asyncPoolStage = AsyncConnectionPoolSupport.createBoundedObjectPoolAsync((Supplier) () -> {
                    if (client instanceof RedisClusterClient) {
                        return CompletableFuture.completedFuture(((RedisClusterClient) client).connect(new ByteArrayCodec()));
                    }
                    if (client instanceof RedisClient) {
                        return CompletableFuture.completedFuture(((RedisClient) client).connect(new ByteArrayCodec()));
                    }
                    throw new ConfigurationException("Invalid Redis connection");
                },
                asyncConfig
        );
        asyncPool = asyncPoolStage.toCompletableFuture().join();
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
    public <T> T get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
        byte[] serializedKey = serializeKey(key);
        try {
            return asyncPool.acquire().thenCompose(connection -> {
                try {
                    RedisStringCommands<byte[], byte[]> commands = getRedisStringCommands(connection);
                    return CompletableFuture.completedFuture(get(serializedKey, requiredType, supplier, commands));
                } finally {
                    asyncPool.release(connection);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return null;
        }
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
        });
    }

    private List<byte[]> allKeys(RedisKeyCommands<byte[], byte[]> commands, byte[] pattern) {
        return commands.keys(pattern);
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
        try {
            return asyncPool.acquire().thenCompose(connection -> {
                try {
                    RedisStringCommands<byte[], byte[]> stringCommands = getRedisStringCommands(connection);
                    RedisKeyCommands<byte[], byte[]> keyCommands = getRedisKeyCommands(connection);
                    return CompletableFuture.completedFuture(getValue(requiredType, serializedKey, stringCommands, keyCommands));
                } finally {
                    asyncPool.release(connection);
                }
            }).get();
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
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

        try {
            asyncPool.acquire().thenAccept(connection -> {
                try {
                    RedisStringCommands<byte[], byte[]> stringCommands = getRedisStringCommands(connection);
                    RedisKeyCommands<byte[], byte[]> keyCommands = getRedisKeyCommands(connection);
                    putValue(serializedKey, serialized, stringCommands, keyCommands);
                } finally {
                    asyncPool.release(connection);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <T> void putValue(byte[] serializedKey,
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
            return asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> commands = getRedisStringAsyncCommands(connection);

                return commands.get(serializedKey).thenCompose(data -> {
                    if (data != null) {
                        return getWithExpire(requiredType, serializedKey, data);
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }).whenComplete((data, ex) -> {
                    asyncPool.release(connection);
                });
            });
        }

        @Override
        public <T> CompletableFuture<T> get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
            byte[] serializedKey = serializeKey(key);
            return asyncPool.acquire().thenCompose(connection -> {
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
                    return putFromSupplier(serializedKey, supplier);
                }).whenComplete((data, ex) -> {
                    asyncPool.release(connection);
                });
            });
        }

        @Override
        public <T> CompletableFuture<Optional<T>> putIfAbsent(Object key, T value) {
            byte[] serializedKey = serializeKey(key);
            return asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> stringCommands = getRedisStringAsyncCommands(connection);
                return stringCommands.get(serializedKey).thenCompose(data -> {
                    if (data != null) {
                        return getWithExpire(Argument.of((Class<T>) value.getClass()), serializedKey, data);
                    }
                    Optional<byte[]> serialized = valueSerializer.serialize(value);
                    if (serialized.isPresent()) {
                        return putWithExpire(serializedKey, serialized.get(), value).thenApply(ignore -> Optional.of(value));
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }).whenComplete((data, ex) -> {
                    asyncPool.release(connection);
                });
            });
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
               return commands.keys(getKeysPattern().getBytes(redisCacheConfiguration.getCharset()))
                        .thenCompose(keys -> deleteByKeys(keys.toArray(new byte[keys.size()][])))
                        .whenComplete((data, ex) -> { asyncPool.release(connection); });
            });
        }

        private CompletableFuture<Boolean> deleteByKeys(byte[]... serializedKey) {
            return asyncPool.acquire().thenCompose(connection -> {
                RedisKeyAsyncCommands<byte[], byte[]> commands = getRedisKeyAsyncCommands(connection);
                return commands.del(serializedKey)
                        .thenApply(keysDeleted -> keysDeleted > 0)
                        .whenComplete((data, ex) -> { asyncPool.release(connection); });
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

        private <T> CompletionStage<Optional<T>> getWithExpire(Argument<T> requiredType, byte[] serializedKey, byte[] data) {
            Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
            if (expireAfterAccess != null && deserialized.isPresent()) {
                return asyncPool.acquire().thenCompose(connection -> {
                    RedisKeyAsyncCommands<byte[], byte[]> commands = getRedisKeyAsyncCommands(connection);
                    return commands.expire(serializedKey, expireAfterAccess)
                            .thenApply(ignore -> deserialized)
                            .whenComplete((result, ex) -> { asyncPool.release(connection); });
                });
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
            } catch (Throwable e) {
                completableFuture.completeExceptionally(e);
            }
            return completableFuture;
        }

        private CompletionStage<Boolean> putWithExpire(byte[] serializedKey, byte[] serialized, Object value) {
            return asyncPool.acquire().thenCompose(connection -> {
                RedisStringAsyncCommands<byte[], byte[]> commands = getRedisStringAsyncCommands(connection);
                if (expireAfterWritePolicy != null) {
                    return commands.psetex(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value), serialized)
                            .whenComplete((result, ex) -> { asyncPool.release(connection); })
                            .thenApply(isOK());
                } else {
                    return commands.set(serializedKey, serialized)
                            .whenComplete((result, ex) -> { asyncPool.release(connection); })
                            .thenApply(isOK());
                }
            });
        }

        private Function<String, Boolean> isOK() {
            return "OK"::equals;
        }
    }
}
