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

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.SyncCache;
import io.micronaut.cache.serialize.DefaultStringKeySerializer;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.configuration.lettuce.cache.expiration.ConstantExpirationAfterWritePolicy;
import io.micronaut.configuration.lettuce.cache.expiration.ExpirationAfterWritePolicy;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.convert.exceptions.ConversionErrorException;
import io.micronaut.core.serialize.JdkSerializer;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.PreDestroy;
import java.time.Duration;
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
@Requires(classes = SyncCache.class)
public class RedisCache implements SyncCache<StatefulConnection<?, ?>>, AutoCloseable {
    private final RedisCacheConfiguration redisCacheConfiguration;
    private final ObjectSerializer keySerializer;
    private final ObjectSerializer valueSerializer;
    private final ExpirationAfterWritePolicy expireAfterWritePolicy;
    private final Long expireAfterAccess;
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
            ConversionService<?> conversionService,
            BeanLocator beanLocator
    ) {
        if (redisCacheConfiguration == null) {
            throw new IllegalArgumentException("Redis cache configuration cannot be null");
        }

        this.redisCacheConfiguration = redisCacheConfiguration;
        this.expireAfterWritePolicy = configureExpirationAfterWritePolicy(redisCacheConfiguration, beanLocator);

        this.expireAfterAccess = redisCacheConfiguration
                .getExpireAfterAccess()
                .map(Duration::toMillis)
                .orElse(defaultRedisCacheConfiguration.getExpireAfterAccess().map(Duration::toMillis).orElse(null));

        this.keySerializer = redisCacheConfiguration
                .getKeySerializer()
                .flatMap(beanLocator::findOrInstantiateBean)
                .orElse(
                        defaultRedisCacheConfiguration
                                .getKeySerializer()
                                .flatMap(beanLocator::findOrInstantiateBean)
                                .orElse(newDefaultKeySerializer(redisCacheConfiguration, conversionService))
                );

        this.valueSerializer = redisCacheConfiguration
                .getValueSerializer()
                .flatMap(beanLocator::findOrInstantiateBean)
                .orElse(
                        defaultRedisCacheConfiguration
                                .getValueSerializer()
                                .flatMap(beanLocator::findOrInstantiateBean)
                                .orElse(new JdkSerializer(conversionService))
                );

        Optional<String> server = Optional.ofNullable(
                redisCacheConfiguration
                        .getServer()
                        .orElse(defaultRedisCacheConfiguration.getServer().orElse(null))
        );

        this.connection = RedisConnectionUtil.openBytesRedisConnection(beanLocator, server, "No Redis server configured to allow caching");
        this.asyncCache = new RedisAsyncCache();

        if (connection instanceof StatefulRedisConnection) {
            RedisCommands<byte[], byte[]> sync = ((StatefulRedisConnection<byte[], byte[]>) connection).sync();
            redisStringCommands = sync;
            redisKeyCommands = sync;
            RedisAsyncCommands<byte[], byte[]> async = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
            redisKeyAsyncCommands = async;
            redisStringAsyncCommands = async;
        } else if (connection instanceof StatefulRedisClusterConnection) {
            RedisAdvancedClusterCommands<byte[], byte[]> sync = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync();
            redisStringCommands = sync;
            redisKeyCommands = sync;
            RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
            redisKeyAsyncCommands = async;
            redisStringAsyncCommands = async;
        } else {
            throw new ConfigurationException("Invalid Redis connection");
        }
    }

    private ExpirationAfterWritePolicy configureExpirationAfterWritePolicy(RedisCacheConfiguration redisCacheConfiguration, BeanLocator beanLocator) {
        if (redisCacheConfiguration.getExpireAfterWrite().isPresent()) {
            Duration expiration = redisCacheConfiguration.getExpireAfterWrite().get();
            return new ConstantExpirationAfterWritePolicy(expiration.toMillis());
        } else if (redisCacheConfiguration.getExpirationAfterWritePolicy().isPresent()) {
            return (ExpirationAfterWritePolicy) redisCacheConfiguration
                    .getExpirationAfterWritePolicy()
                    .flatMap(className -> findExpirationAfterWritePolicyBean(beanLocator, className))
                    .get();
        }
        return null;
    }

    private Optional<?> findExpirationAfterWritePolicyBean(BeanLocator beanLocator, String className) {
        try {
            Optional<?> bean = beanLocator.findOrInstantiateBean(Class.forName(className));
            if (bean.isPresent()) {
                if (bean.get() instanceof ExpirationAfterWritePolicy) {
                    return bean;
                }
                throw new ConfigurationException("Redis expiration-after-write-policy was not of type ExpirationAfterWritePolicy");
            } else {
                throw new ConfigurationException("Redis expiration-after-write-policy was not found");
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Redis expiration-after-write-policy was not found");
        }
    }

    @Override
    public String getName() {
        return redisCacheConfiguration.getCacheName();
    }

    @Override
    public StatefulConnection<?, ?> getNativeCache() {
        return connection;
    }

    @Override
    public <T> Optional<T> get(Object key, Argument<T> requiredType) {
        byte[] serializedKey = serializeKey(key);
        return getValue(requiredType, serializedKey);
    }

    @Override
    public <T> T get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
        byte[] serializedKey = serializeKey(key);
        byte[] data = redisStringCommands.get(serializedKey);
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

    public <K> Map<K, Object> get(Collection<K> keys) {
      return get(keys, Argument.of(Object.class));
    }

    public <K, T> Map<K, T> get(Collection<K> keys, Argument<T> requiredType) {
        Map<byte[], K> serializedKeyMap = new HashMap<>(keys.size());
        byte[][] serializedKeys = new byte[keys.size()][];
        int i = 0;
        for (K key: keys) {
            final byte[] serializedKey = this.serializeKey(key);
            serializedKeys[i++] = serializedKey;
            serializedKeyMap.put(serializedKey, key);
        };

        List<KeyValue<byte[], byte[]>> data = redisStringCommands.mget(serializedKeys);
        final Map<K, T> results;
        if (data != null) {
            results = new LinkedHashMap<>(data.size());
            for (KeyValue<byte[], byte[]> result: data) {
                K originalKey = serializedKeyMap.get(result.getKey());
                if (result.hasValue()) {
                  final T deserialized = valueSerializer.deserialize(result.getValue(), requiredType)
                      .orElseThrow(() ->
                          new ConversionErrorException(requiredType,
                              new IllegalArgumentException("Cannot convert cached value for [" + originalKey + "] to target type: " + requiredType.getType() + ". Considering defining a TypeConverter bean to handle this case.")));
                  results.put(originalKey, deserialized);
                } else {
                  results.put(originalKey, null);
                }
            }
        } else {
            results = new LinkedHashMap<>();
        }

        return results;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> putIfAbsent(Object key, T value) {
        if (value == null) {
            return Optional.empty();
        }

        byte[] serializedKey = serializeKey(key);
        Optional<T> existing = getValue(Argument.of((Class<T>) value.getClass()), serializedKey);
        if (!existing.isPresent()) {
            putValue(serializedKey, value);
            return Optional.empty();
        } else {
            return existing;
        }
    }

    @Override
    public void put(Object key, Object value) {
        byte[] serializedKey = serializeKey(key);
        putValue(serializedKey, value);
    }

    public void put(Map<?, ?> values) {
        if (CollectionUtils.isNotEmpty(values)) {
            if (expireAfterWritePolicy != null) {
                values.forEach(this::put);
            } else {
                Map<byte[], byte[]> toSave = new HashMap<>(values.size());
                List<byte[]> toDelete = new ArrayList<>();
                values.forEach((key, value) -> {
                    byte[] serializedKey = serializeKey(key);
                    Optional<byte[]> serialized = valueSerializer.serialize(value);
                    if (serialized.isPresent()) {
                        toSave.put(serializedKey, serialized.get());
                    } else {
                        toDelete.add(serializedKey);
                    }
                });

                if (CollectionUtils.isNotEmpty(toSave)) {
                    redisStringCommands.mset(toSave);
                }
                if (CollectionUtils.isNotEmpty(toDelete)) {
                    redisKeyCommands.del(toDelete.toArray(new byte[toDelete.size()][]));
                }
            }
        }
    }

    @Override
    public void invalidate(Object key) {
        byte[] serializedKey = serializeKey(key);
        redisKeyCommands.del(serializedKey);
    }

    public void invalidate(Collection<?> keys) {
        byte[][] serializedKeys = keys.stream().map(this::serializeKey).toArray(byte[][]::new);
        redisKeyCommands.del(serializedKeys);
    }

    @Override
    public void invalidateAll() {
        List<byte[]> keys = redisKeyCommands.keys(getKeysPattern().getBytes(redisCacheConfiguration.getCharset()));
        if (!keys.isEmpty()) {
            redisKeyCommands.del(keys.toArray(new byte[keys.size()][]));
        }
    }

    @Override
    public AsyncCache<StatefulConnection<?, ?>> async() {
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
     * @return The default keys pattern.
     */
    protected String getKeysPattern() {
        return getName() + ":*";
    }

    /**
     * Place the value in the cache.
     *
     * @param serializedKey serializedKey
     * @param value         value
     * @param <T>           type of the value
     */
    protected <T> void putValue(byte[] serializedKey, T value) {
        Optional<byte[]> serialized = valueSerializer.serialize(value);
        if (serialized.isPresent()) {
            byte[] bytes = serialized.get();
            if (expireAfterWritePolicy != null) {
                redisStringCommands.psetex(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value), bytes);
            } else {
                redisStringCommands.set(serializedKey, bytes);
            }
        } else {
            redisKeyCommands.del(serializedKey);
        }
    }

    /**
     * Serialize the key.
     *
     * @param key The key
     * @return bytes of the object
     */
    protected byte[] serializeKey(Object key) {
        return keySerializer.serialize(key).orElseThrow(() -> new IllegalArgumentException("Key cannot be null"));
    }

    private DefaultStringKeySerializer newDefaultKeySerializer(RedisCacheConfiguration redisCacheConfiguration, ConversionService<?> conversionService) {
        return new DefaultStringKeySerializer(redisCacheConfiguration.getCacheName(), redisCacheConfiguration.getCharset(), conversionService);
    }

    @PreDestroy
    @Override
    public void close() {
        connection.close();
    }

    /**
     * Redis Async cache implementation.
     */
    protected class RedisAsyncCache implements AsyncCache<StatefulConnection<?, ?>> {

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
                        return redisKeyAsyncCommands.expire(serializedKey, expireAfterAccess).thenApply(ignore -> deserialized.get());
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
        public StatefulConnection<?, ?> getNativeCache() {
            return RedisCache.this.getNativeCache();
        }

        private <T> CompletionStage<Optional<T>> getWithExpire(Argument<T> requiredType, byte[] serializedKey, byte[] data) {
            Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
            if (expireAfterAccess != null && deserialized.isPresent()) {
                return redisKeyAsyncCommands.expire(serializedKey, expireAfterAccess)
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
