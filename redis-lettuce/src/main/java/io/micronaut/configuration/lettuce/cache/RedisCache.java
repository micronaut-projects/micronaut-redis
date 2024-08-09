/*
 * Copyright 2017-2020 original authors
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

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.SyncCache;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An implementation of {@link SyncCache} for Lettuce / Redis.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@EachBean(RedisCacheConfiguration.class)
@Requires(classes = SyncCache.class, property = RedisSetting.REDIS_POOL + ".enabled", defaultValue = StringUtils.FALSE, notEquals = StringUtils.TRUE)
public class RedisCache extends AbstractRedisCache<StatefulConnection<byte[], byte[]>> {
    private final RedisAsyncCache asyncCache;
    private final StatefulConnection<byte[], byte[]> connection;
    private final RedisKeyCommands<byte[], byte[]> redisKeyCommands;
    private final RedisStringCommands<byte[], byte[]> redisStringCommands;
    private final RedisKeyAsyncCommands<byte[], byte[]> redisKeyAsyncCommands;
    private final RedisStringAsyncCommands<byte[], byte[]> redisStringAsyncCommands;

    /**
     * Creates a new redis cache for the given arguments.
     *
     * @param defaultRedisCacheConfiguration The default configuration
     * @param redisCacheConfiguration        The configuration
     * @param conversionService              The conversion service
     * @param beanLocator                    The bean locator used to discover the redis connection from the configuration
     */
    @SuppressWarnings("unchecked")
    public RedisCache(
            DefaultRedisCacheConfiguration defaultRedisCacheConfiguration,
            RedisCacheConfiguration redisCacheConfiguration,
            ConversionService conversionService,
            BeanLocator beanLocator
    ) {
        super(defaultRedisCacheConfiguration, redisCacheConfiguration, conversionService, beanLocator);
        Optional<String> server = Optional.ofNullable(
                redisCacheConfiguration
                        .getServer()
                        .orElse(defaultRedisCacheConfiguration.getServer().orElse(null))
        );

        this.connection = RedisConnectionUtil.openBytesRedisConnection(beanLocator, server, "No Redis server configured to allow caching");
        this.asyncCache = new RedisAsyncCache();

        redisKeyCommands = getRedisKeyCommands(connection);
        redisStringCommands = getRedisStringCommands(connection);
        redisKeyAsyncCommands = getRedisKeyAsyncCommands(connection);
        redisStringAsyncCommands = getRedisStringAsyncCommands(connection);
    }

    @Override
    public String getName() {
        return redisCacheConfiguration.getCacheName();
    }

    @Override
    public StatefulConnection<byte[], byte[]> getNativeCache() {
        return connection;
    }

    @Override
    public <T> T get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
        byte[] serializedKey = serializeKey(key);
        return get(serializedKey, requiredType, supplier, redisStringCommands);
    }

    @Override
    public void invalidate(Object key) {
        byte[] serializedKey = serializeKey(key);
        redisKeyCommands.del(serializedKey);
    }

    @Override
    public void invalidateAll() {
        List<byte[]> keys = redisKeyCommands.keys(getKeysPattern().getBytes(redisCacheConfiguration.getCharset()));
        if (!keys.isEmpty()) {
            redisKeyCommands.del(keys.toArray(new byte[keys.size()][]));
        }
    }

    @Override
    public AsyncCache<StatefulConnection<byte[], byte[]>> async() {
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
        byte[] data = redisStringCommands.get(serializedKey);
        if (expireAfterAccess != null) {
            redisKeyCommands.pexpire(serializedKey, expireAfterAccess);
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
        putValue(serializedKey,
                serialized,
                expireAfterWritePolicy,
                redisStringCommands,
                redisKeyCommands,
                value);
    }

    @Override
    @NonNull
    @SuppressWarnings("java:S1185") // This is here for binary compatibility
    public <T> Optional<T> get(Object key, Argument<T> requiredType) {
        return super.get(key, requiredType);
    }

    @Override
    @SuppressWarnings("java:S1185") // This is here for binary compatibility
    public void put(Object key, Object value) {
        super.put(key, value);
    }

    @Override
    @NonNull
    @SuppressWarnings("java:S1185") // This is here for binary compatibility
    public <T> Optional<T> putIfAbsent(Object key, T value) {
        return super.putIfAbsent(key, value);
    }

    @Override
    @SuppressWarnings("java:S1185") // This is here for binary compatibility
    protected String getKeysPattern() {
        return super.getKeysPattern();
    }

    @Override
    @SuppressWarnings("java:S1185") // This is here for binary compatibility
    protected byte[] serializeKey(Object key) {
        return super.serializeKey(key);
    }

    @PreDestroy
    @Override
    public void close() {
        connection.close();
    }

    /**
     * Redis Async cache implementation.
     */
    protected class RedisAsyncCache implements AsyncCache<StatefulConnection<byte[], byte[]>> {

        @Override
        public <T> CompletableFuture<Optional<T>> get(Object key, Argument<T> requiredType) {
            byte[] serializedKey = serializeKey(key);
            return redisStringAsyncCommands.get(serializedKey).thenCompose(data -> {
                if (data != null) {
                    return getWithExpire(requiredType, serializedKey, data);
                }
                return CompletableFuture.completedFuture(Optional.empty());
            }).toCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
            byte[] serializedKey = serializeKey(key);
            return redisStringAsyncCommands.get(serializedKey).thenCompose(data -> {
                if (data != null) {
                    Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
                    boolean hasValue = deserialized.isPresent();
                    if (expireAfterAccess != null && hasValue) {
                        final long expireAfterAccessInSeconds = expireAfterAccess / 1000;
                        return redisKeyAsyncCommands.expire(serializedKey, expireAfterAccessInSeconds).thenApply(ignore -> deserialized.get());
                    } else if (hasValue) {
                        return CompletableFuture.completedFuture(deserialized.get());
                    }
                }
                return putFromSupplier(serializedKey, supplier);
            }).toCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<Optional<T>> putIfAbsent(Object key, T value) {
            byte[] serializedKey = serializeKey(key);
            return redisStringAsyncCommands.get(serializedKey).thenCompose(data -> {
                if (data != null) {
                    return getWithExpire(Argument.of((Class<T>) value.getClass()), serializedKey, data);
                }
                Optional<byte[]> serialized = valueSerializer.serialize(value);
                if (serialized.isPresent()) {
                    return putWithExpire(serializedKey, serialized.get(), value).thenApply(ignore -> Optional.of(value));
                }
                return CompletableFuture.completedFuture(Optional.empty());
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
            return redisKeyAsyncCommands.keys(getKeysPattern().getBytes(redisCacheConfiguration.getCharset()))
                    .thenCompose(keys -> deleteByKeys(keys.toArray(new byte[keys.size()][])))
                    .toCompletableFuture();
        }

        private CompletableFuture<Boolean> deleteByKeys(byte[]... serializedKey) {
            return redisKeyAsyncCommands.del(serializedKey)
                    .thenApply(keysDeleted -> keysDeleted > 0)
                    .toCompletableFuture();
        }

        @Override
        public String getName() {
            return RedisCache.this.getName();
        }

        @Override
        public StatefulConnection<byte[], byte[]> getNativeCache() {
            return RedisCache.this.getNativeCache();
        }

        private <T> CompletionStage<Optional<T>> getWithExpire(Argument<T> requiredType, byte[] serializedKey, byte[] data) {
            Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
            if (expireAfterAccess != null && deserialized.isPresent()) {
                final long expireAfterAccessInSeconds = expireAfterAccess / 1000;
                return redisKeyAsyncCommands.expire(serializedKey, expireAfterAccessInSeconds)
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
            } catch (Throwable e) {
                completableFuture.completeExceptionally(e);
            }
            return completableFuture;
        }

        private CompletionStage<Boolean> putWithExpire(byte[] serializedKey, byte[] serialized, Object value) {
            if (expireAfterWritePolicy != null) {
                return redisStringAsyncCommands.psetex(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value), serialized).thenApply(isOK());
            } else {
                return redisStringAsyncCommands.set(serializedKey, serialized).thenApply(isOK());
            }
        }

        private Function<String, Boolean> isOK() {
            return "OK"::equals;
        }

    }
}
