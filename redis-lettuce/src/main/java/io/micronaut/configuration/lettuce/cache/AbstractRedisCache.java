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
import io.micronaut.core.serialize.JdkSerializer;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.core.type.Argument;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An abstract class implementing SyncCache for the redis.
 * Author: Graeme Rocher, Kovalov Illia
 */
public abstract class AbstractRedisCache<T> implements SyncCache<T>, AutoCloseable {
    protected final ObjectSerializer keySerializer;
    protected final ObjectSerializer valueSerializer;
    protected final RedisCacheConfiguration redisCacheConfiguration;
    protected final ExpirationAfterWritePolicy expireAfterWritePolicy;
    protected final Long expireAfterAccess;

    public AbstractRedisCache(
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

    protected <T> T get(byte[] key, Argument<T> requiredType, Supplier<T> supplier, RedisStringCommands<byte[], byte[]> commands){
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

    protected RedisStringAsyncCommands<byte[], byte[]> getRedisStringAsyncCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisStringAsyncCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
        } else throw new ConfigurationException("Invalid Redis connection");
        return commands;
    }

    protected RedisKeyAsyncCommands<byte[], byte[]> getRedisKeyAsyncCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisKeyAsyncCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
        } else throw new ConfigurationException("Invalid Redis connection");
        return commands;
    }

    protected RedisStringCommands<byte[], byte[]> getRedisStringCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisStringCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).sync();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync();
        } else throw new ConfigurationException("Invalid Redis connection");
        return commands;
    }

    protected RedisKeyCommands<byte[], byte[]> getRedisKeyCommands(StatefulConnection<byte[], byte[]> connection) {
        RedisKeyCommands<byte[], byte[]> commands;
        if (connection instanceof StatefulRedisConnection) {
            commands = ((StatefulRedisConnection<byte[], byte[]>) connection).sync();
        } else if (connection instanceof StatefulRedisClusterConnection) {
            commands = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync();
        } else throw new ConfigurationException("Invalid Redis connection");
        return commands;
    }

    protected abstract <T> Optional<T> getValue(Argument<T> requiredType, byte[] serializedKey);

    protected abstract <T> void putValue(byte[] serializedKey, T value);

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

    private DefaultStringKeySerializer newDefaultKeySerializer(RedisCacheConfiguration redisCacheConfiguration, ConversionService<?> conversionService) {
        return new DefaultStringKeySerializer(redisCacheConfiguration.getCacheName(), redisCacheConfiguration.getCharset(), conversionService);
    }
}
