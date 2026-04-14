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

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micronaut.cache.SyncCache;
import io.micronaut.cache.serialize.DefaultStringKeySerializer;
import io.micronaut.configuration.lettuce.cache.expiration.ConstantExpirationAfterWritePolicy;
import io.micronaut.configuration.lettuce.cache.expiration.ExpirationAfterWritePolicy;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.convert.exceptions.ConversionErrorException;
import io.micronaut.core.serialize.JdkSerializer;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.core.type.Argument;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.Optional;

/**
 * An abstract class implementing SyncCache for the redis.
 *
 * @author Graeme Rocher, Kovalov Illia
 * @param <C> – The native cache implementation
 * @since 5.3.0
 */
public abstract class AbstractRedisCache<C> implements SyncCache<C>, AutoCloseable {

    public static final String INVALID_REDIS_CONNECTION_MESSAGE = "Invalid Redis connection";

    protected final ObjectSerializer keySerializer;
    protected final ObjectSerializer valueSerializer;
    protected final RedisCacheConfiguration redisCacheConfiguration;
    protected final ExpirationAfterWritePolicy expireAfterWritePolicy;
    protected final Long expireAfterAccess;
    protected final Long invalidateScanCount;

    protected AbstractRedisCache(
            DefaultRedisCacheConfiguration defaultRedisCacheConfiguration,
            RedisCacheConfiguration redisCacheConfiguration,
            ConversionService conversionService,
            BeanLocator beanLocator
    ) {
        if (redisCacheConfiguration == null) {
            throw new IllegalArgumentException("Redis cache configuration cannot be null");
        }

        this.redisCacheConfiguration = redisCacheConfiguration;
        this.expireAfterWritePolicy = configureExpirationAfterWritePolicy(redisCacheConfiguration, beanLocator);


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

        this.expireAfterAccess = redisCacheConfiguration
                .getExpireAfterAccess()
                .map(Duration::toMillis)
                .orElse(defaultRedisCacheConfiguration.getExpireAfterAccess().map(Duration::toMillis).orElse(null));

        this.invalidateScanCount = redisCacheConfiguration.getInvalidateScanCount().orElse(100L);
    }

    @Override
    public <T> Optional<T> get(Object key, Argument<T> requiredType) {
        byte[] serializedKey = serializeKey(key);
        return getValue(requiredType, serializedKey);
    }

    @Override
    public void put(Object key, Object value) {
        byte[] serializedKey = serializeKey(key);
        putValue(serializedKey, value);
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

    /**
     * @param key
     * @param requiredType
     * @param supplier
     * @param commands
     * @param <T> concrete type
     * @return value from the cache
     */
    protected <T> T get(byte[] key, Argument<T> requiredType, Supplier<T> supplier, RedisStringCommands<byte[], byte[]> commands) {
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

    /**
     * @param connection
     * @return RedisStringAsyncCommands
     */
    protected RedisStringAsyncCommands<byte[], byte[]> getRedisStringAsyncCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisStringAsyncCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
        } else {
            throw new ConfigurationException(INVALID_REDIS_CONNECTION_MESSAGE);
        }
        return commands;
    }

    /**
     * @param connection
     * @return RedisKeyAsyncCommands
     */
    protected RedisKeyAsyncCommands<byte[], byte[]> getRedisKeyAsyncCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisKeyAsyncCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
        } else {
            throw new ConfigurationException(INVALID_REDIS_CONNECTION_MESSAGE);
        }
        return commands;
    }

    /**
     * @param connection
     * @return RedisStringCommands
     */
    protected RedisStringCommands<byte[], byte[]> getRedisStringCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisStringCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).sync();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync();
        } else {
            throw new ConfigurationException(INVALID_REDIS_CONNECTION_MESSAGE);
        }
        return commands;
    }

    /**
     * @param connection
     * @return RedisKeyCommands
     */
    protected RedisKeyCommands<byte[], byte[]> getRedisKeyCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisKeyCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).sync();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync();
        } else {
            throw new ConfigurationException(INVALID_REDIS_CONNECTION_MESSAGE);
        }
        return commands;
    }

    /**
     * @param requiredType
     * @param serializedKey
     * @param <T> concrete type
     * @return An Optional Value
     */
    protected abstract <T> Optional<T> getValue(Argument<T> requiredType, byte[] serializedKey);

    /**
     * @param serializedKey
     * @param value
     * @param <T> concrete type
     */
    protected abstract <T> void putValue(byte[] serializedKey, T value);

    /**
     * @param serializedKey
     * @param serialized
     * @param policy
     * @param redisStringCommands
     * @param redisKeyCommands
     * @param value
     * @param <T> concrete type
     */
    protected <T> void putValue(byte[] serializedKey, Optional<byte[]> serialized, ExpirationAfterWritePolicy policy, RedisStringCommands<byte[], byte[]> redisStringCommands, RedisKeyCommands<byte[], byte[]> redisKeyCommands, T value) {
        if (serialized.isPresent()) {
            byte[] bytes = serialized.get();
            if (policy != null) {
                redisStringCommands.psetex(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value), bytes);
            } else {
                redisStringCommands.set(serializedKey, bytes);
            }
        } else {
            redisKeyCommands.del(serializedKey);
        }
    }

    /**
     * Resolve the values for the given keys and convert them to the required type.
     *
     * @param keys               The cache keys
     * @param requiredType       The required type
     * @param redisStringCommands The redis string commands
     * @param redisKeyCommands   The redis key commands
     * @param <K>                The type of the unserialized key
     * @param <T>                The concrete type
     * @return An ordered map containing all requested keys in the original order
     */
    protected <K, T> Map<K, T> getValues(
        @NonNull Collection<K> keys,
        @NonNull Argument<T> requiredType,
        RedisStringCommands<byte[], byte[]> redisStringCommands,
        RedisKeyCommands<byte[], byte[]> redisKeyCommands
    ) {
        if (keys.isEmpty()) {
            return new LinkedHashMap<>();
        }

        List<K> orderedKeys = new ArrayList<>(keys.size());
        byte[][] serializedKeys = new byte[keys.size()][];
        int index = 0;
        for (K key : keys) {
            orderedKeys.add(key);
            serializedKeys[index++] = serializeKey(key);
        }

        List<KeyValue<byte[], byte[]>> values = redisStringCommands.mget(serializedKeys);
        Map<K, T> resolved = new LinkedHashMap<>(orderedKeys.size());
        for (int i = 0; i < orderedKeys.size(); i++) {
            K key = orderedKeys.get(i);
            KeyValue<byte[], byte[]> value = values.get(i);
            if (value != null && value.hasValue()) {
                T deserialized = valueSerializer.deserialize(value.getValue(), requiredType)
                    .orElseThrow(() -> new ConversionErrorException(requiredType,
                        new IllegalArgumentException("Cannot convert cached value for [" + key + "] to target type: " + requiredType.getType())));
                resolved.put(key, deserialized);
                if (expireAfterAccess != null) {
                    redisKeyCommands.pexpire(serializedKeys[i], expireAfterAccess);
                }
            } else {
                resolved.put(key, null);
            }
        }
        return resolved;
    }

    /**
     * Cache the specified values in bulk.
     *
     * @param values The values to store
     * @param redisStringCommands The redis string commands
     * @param redisKeyCommands The redis key commands
     */
    protected void putValues(
        @NonNull Map<?, ?> values,
        RedisStringCommands<byte[], byte[]> redisStringCommands,
        RedisKeyCommands<byte[], byte[]> redisKeyCommands
    ) {
        if (values.isEmpty()) {
            return;
        }

        Map<byte[], byte[]> toSave = new LinkedHashMap<>(values.size());
        Map<byte[], Long> toExpire = expireAfterWritePolicy == null ? null : new LinkedHashMap<>(values.size());
        List<byte[]> toDelete = new ArrayList<>(values.size());
        values.forEach((key, value) -> {
            byte[] serializedKey = serializeKey(key);
            if (value == null) {
                toDelete.add(serializedKey);
                return;
            }

            Optional<byte[]> serialized = valueSerializer.serialize(value);
            if (serialized.isPresent()) {
                toSave.put(serializedKey, serialized.get());
                if (toExpire != null) {
                    toExpire.put(serializedKey, expireAfterWritePolicy.getExpirationAfterWrite(value));
                }
            } else {
                toDelete.add(serializedKey);
            }
        });

        if (!toSave.isEmpty()) {
            redisStringCommands.mset(toSave);
            if (toExpire != null) {
                toExpire.forEach(redisKeyCommands::pexpire);
            }
        }
        if (!toDelete.isEmpty()) {
            redisKeyCommands.del(toDelete.toArray(new byte[0][]));
        }
    }

    /**
     * Invalidate the values for the given keys.
     *
     * @param keys The keys to invalidate
     * @param redisKeyCommands The redis key commands
     */
    protected void invalidateValues(@NonNull Collection<?> keys, RedisKeyCommands<byte[], byte[]> redisKeyCommands) {
        if (keys.isEmpty()) {
            return;
        }
        byte[][] serializedKeys = keys.stream().map(this::serializeKey).toArray(byte[][]::new);
        redisKeyCommands.del(serializedKeys);
    }

    /**
     * @return The default keys pattern.
     */
    protected String getKeysPattern() {
        return getName() + ":*";
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

    private ExpirationAfterWritePolicy configureExpirationAfterWritePolicy(AbstractRedisCacheConfiguration redisCacheConfiguration, BeanLocator beanLocator) {
        Optional<Duration> expireAfterWrite = redisCacheConfiguration.getExpireAfterWrite();
        Optional<String> expirationAfterWritePolicy = redisCacheConfiguration.getExpirationAfterWritePolicy();
        if (expireAfterWrite.isPresent()) {
            Duration expiration = expireAfterWrite.get();
            return new ConstantExpirationAfterWritePolicy(expiration.toMillis());
        } else if (expirationAfterWritePolicy.isPresent()) {
            Optional<ExpirationAfterWritePolicy> policy = expirationAfterWritePolicy.map(className -> findExpirationAfterWritePolicyBean(beanLocator, className));
            return policy.orElseThrow(() -> new IllegalArgumentException("Unknown policy " + expirationAfterWritePolicy));
        }
        return null;
    }

    private ExpirationAfterWritePolicy findExpirationAfterWritePolicyBean(BeanLocator beanLocator, String className) {
        try {
            Optional<?> bean = beanLocator.findOrInstantiateBean(Class.forName(className));
            if (bean.isPresent()) {
                Object foundBean = bean.get();
                if (foundBean instanceof ExpirationAfterWritePolicy) {
                    return (ExpirationAfterWritePolicy) foundBean;
                }
                throw new ConfigurationException("Redis expiration-after-write-policy was not of type ExpirationAfterWritePolicy");
            } else {
                throw new ConfigurationException("Redis expiration-after-write-policy was not found");
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Redis expiration-after-write-policy was not found");
        }
    }

    private DefaultStringKeySerializer newDefaultKeySerializer(RedisCacheConfiguration redisCacheConfiguration, ConversionService conversionService) {
        return new DefaultStringKeySerializer(redisCacheConfiguration.getCacheName(), redisCacheConfiguration.getCharset(), conversionService);
    }
}
