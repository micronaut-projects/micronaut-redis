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
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.SyncCache;
import io.micronaut.cache.serialize.DefaultStringKeySerializer;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.configuration.lettuce.cache.expiration.ConstantExpirationAfterWritePolicy;
import io.micronaut.configuration.lettuce.cache.expiration.ExpirationAfterWritePolicy;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.serialize.JdkSerializer;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.core.type.Argument;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
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
@Requires(classes = SyncCache.class, property = RedisSetting.REDIS_POOL)
public class RedisConnectionPoolCache implements SyncCache<StatefulConnection<?, ?>>, AutoCloseable {
    private final RedisCacheConfiguration redisCacheConfiguration;
    private final ObjectSerializer keySerializer;
    private final ObjectSerializer valueSerializer;
    private final ExpirationAfterWritePolicy expireAfterWritePolicy;
    private final Long expireAfterAccess;
    private final RedisAsyncCache asyncCache;
    private final ObjectPool<StatefulConnection<byte[], byte[]>> pool;
    private final AsyncPool<StatefulConnection<byte[], byte[]>> asyncPool;
    private final StatefulConnection<byte[], byte[]> connection;

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
        StatefulConnection<Byte[], Byte[]> connection = RedisConnectionUtil.findRedisConnection(beanLocator, server, "No Redis server configured to allow caching");
        this.asyncCache = new RedisAsyncCache();
        AbstractRedisClient client = RedisConnectionUtil.findClient(beanLocator, server, "No Redis server configured to allow caching");
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMinIdle(4);
        config.setMaxIdle(16);
        config.setMaxTotal(32);
        this.pool = ConnectionPoolSupport.createGenericObjectPool((Supplier) () -> {
            if (client instanceof RedisClusterClient) {
                return ((RedisClusterClient) client).connect();
            }
            if (client instanceof RedisClient) {
                return ((RedisClient) client).connect();
            }
            throw new ConfigurationException("Invalid Redis connection");
        }, config);
        BoundedPoolConfig asyncConfig = BoundedPoolConfig.builder().minIdle(4).maxIdle(16).maxTotal(32).build();
        CompletionStage<AsyncPool<StatefulConnection<byte[], byte[]>>> asyncPoolStage = AsyncConnectionPoolSupport.createBoundedObjectPoolAsync((Supplier) () -> {
                    if (client instanceof RedisClusterClient) {
                        return ((RedisClusterClient) client).connect();
                    }
                    if (client instanceof RedisClient) {
                        return ((RedisClient) client).connect();
                    }
                    throw new ConfigurationException("Invalid Redis connection");
                },
                asyncConfig
        );
        asyncPool = asyncPoolStage.toCompletableFuture().join();
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
        try {
            StatefulConnection<byte[], byte[]> conn = pool.borrowObject();
            try {
                if (conn instanceof StatefulRedisConnection) {
                    return get(serializedKey, requiredType, supplier, ((StatefulRedisConnection<byte[], byte[]>) conn).sync());
                } else if (conn instanceof StatefulRedisClusterConnection) {
                    return get(serializedKey, requiredType, supplier, ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync());
                } else {
                    throw new ConfigurationException("Invalid Redis connection");
                }
            } finally {
                pool.returnObject(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private <T> T get(byte[] key, Argument<T> requiredType, Supplier<T> supplier, RedisStringCommands<byte[], byte[]> commands){
        byte[] data = commands.get(key);
        if (data != null) {
            Optional<T> deserialized = valueSerializer.deserialize(data, requiredType);
            if (deserialized.isPresent()) {
                return deserialized.get();
            }
        }

        T value = supplier.get();
        putValue(key, value);
        return value;
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

    @Override
    public void invalidate(Object key) {
        byte[] serializedKey = serializeKey(key);

        try {
            StatefulConnection<byte[], byte[]> conn = pool.borrowObject();
            try {
                RedisKeyCommands<byte[], byte[]> commands;
                if (conn instanceof StatefulRedisConnection) {
                    commands = ((StatefulRedisConnection<byte[], byte[]>) conn).sync();
                } else if (conn instanceof StatefulRedisClusterConnection) {
                    commands = ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync();
                } else {
                    throw new ConfigurationException("Invalid Redis connection");
                }
                invalidate(Collections.singletonList(serializedKey), commands);
            }
            finally {
                pool.returnObject(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void invalidate(List<byte[]> key, RedisKeyCommands<byte[], byte[]> commands) {
        commands.del(key.toArray(new byte[0][]));
    }

    @Override
    public void invalidateAll() {
        try {
            StatefulConnection<byte[], byte[]> conn = pool.borrowObject();
            RedisKeyCommands<byte[], byte[]> commands;
            if (conn instanceof StatefulRedisConnection) {
                commands = ((StatefulRedisConnection<byte[], byte[]>) conn).sync();
            } else if (conn instanceof StatefulRedisClusterConnection) {
                commands = ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync();
            } else {
                throw new ConfigurationException("Invalid Redis connection");
            }
            List<byte[]> keys = allKeys(commands, getKeysPattern().getBytes(redisCacheConfiguration.getCharset()));
            if (!keys.isEmpty()) {
                invalidate(keys, commands);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<byte[]> allKeys(RedisKeyCommands<byte[], byte[]> commands, byte[] pattern) {
        return commands.keys(pattern);
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
        try {
            StatefulConnection<byte[], byte[]> conn = pool.borrowObject();
            try {
                RedisStringCommands<byte[], byte[]> stringCommands;
                RedisKeyCommands<byte[], byte[]> keyCommands;
                if (conn instanceof StatefulRedisConnection) {
                    RedisCommands<byte[], byte[]> commands = ((StatefulRedisConnection<byte[], byte[]>) conn).sync();
                    stringCommands = commands;
                    keyCommands = commands;
                } else if (conn instanceof StatefulRedisClusterConnection) {
                    RedisAdvancedClusterCommands<byte[], byte[]> commands = ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync();
                    stringCommands = commands;
                    keyCommands = commands;
                } else {
                    throw new ConfigurationException("Invalid Redis connection");
                }
                return getValue(requiredType, serializedKey, stringCommands, keyCommands);
            } finally {
                pool.returnObject(conn);
            }
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

        try {
            StatefulConnection<byte[], byte[]> conn = pool.borrowObject();
            try {
                RedisStringCommands<byte[], byte[]> stringCommands;
                RedisKeyCommands<byte[], byte[]> keyCommands;
                if (conn instanceof StatefulRedisConnection) {
                    RedisCommands<byte[], byte[]> commands = ((StatefulRedisConnection<byte[], byte[]>) conn).sync();
                    stringCommands = commands;
                    keyCommands = commands;
                } else if (conn instanceof StatefulRedisClusterConnection) {
                    RedisAdvancedClusterCommands<byte[], byte[]> commands = ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync();
                    stringCommands = commands;
                    keyCommands = commands;
                } else {
                    throw new ConfigurationException("Invalid Redis connection");
                }
                putValue(serializedKey, serialized, stringCommands, keyCommands);
            } finally {
                pool.returnObject(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <T> void putValue(byte[] serializedKey,
                              Optional<byte[]> value,
                              RedisStringCommands<byte[], byte[]> stringCommands,
                              RedisKeyCommands<byte[], byte[]> keyCommands
    ) {
        if (value.isPresent()) {
            byte[] bytes = value.get();
            if (expireAfterWritePolicy != null) {
                stringCommands.psetex(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value), bytes);
            } else {
                stringCommands.set(serializedKey, bytes);
            }
        } else {
            keyCommands.del(serializedKey);
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

        private RedisStringAsyncCommands<byte[], byte[]> getRedisStringAsyncCommands(StatefulConnection<byte[], byte[]> connection) {
            RedisStringAsyncCommands<byte[], byte[]> commands;
            if (connection instanceof StatefulRedisConnection) {
                commands = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
            } else if (connection instanceof StatefulRedisClusterConnection) {
                commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
            } else throw new ConfigurationException("Invalid Redis connection");
            return commands;
        }

        private RedisKeyAsyncCommands<byte[], byte[]> getRedisKeyAsyncCommands(StatefulConnection<byte[], byte[]> connection) {
            RedisKeyAsyncCommands<byte[], byte[]> commands;
            if (connection instanceof StatefulRedisConnection) {
                commands = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
            } else if (connection instanceof StatefulRedisClusterConnection) {
                commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
            } else throw new ConfigurationException("Invalid Redis connection");
            return commands;
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
        public StatefulConnection<?, ?> getNativeCache() {
            return RedisConnectionPoolCache.this.getNativeCache();
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
