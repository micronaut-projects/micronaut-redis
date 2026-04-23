/*
 * Copyright 2017-2024 original authors
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
package io.micronaut.configuration.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Creates optional pooled Redis connections for cases where callers need to run work across
 * multiple underlying Lettuce connections instead of a single long-lived connection bean.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @since 7.0.0
 */
@Factory
@Requires(beans = {DefaultRedisConfiguration.class, DefaultRedisConnectionPoolConfiguration.class})
public class RedisConnectionPoolFactory<K, V> {

    private final RedisCodec<K, V> defaultCodec;

    /**
     * @param defaultCodec The default codec
     */
    public RedisConnectionPoolFactory(@Primary RedisCodec<K, V> defaultCodec) {
        this.defaultCodec = defaultCodec;
    }

    /**
     * Creates a pool of default Redis connections.
     *
     * @param redisClient The Redis client
     * @param config The Redis configuration
     * @param poolConfiguration The pool configuration
     * @return The connection pool
     */
    @Bean(preDestroy = "close")
    @Singleton
    @Primary
    @Requires(beans = RedisClient.class)
    @Requires(missingProperty = RedisSetting.REDIS_URIS)
    public AsyncPool<StatefulRedisConnection<K, V>> redisConnectionPool(
        @Primary RedisClient redisClient,
        @Primary DefaultRedisConfiguration config,
        DefaultRedisConnectionPoolConfiguration poolConfiguration
    ) {
        BoundedPoolConfig boundedPoolConfig = poolConfiguration.getBoundedPoolConfig();
        // wrapConnections=false: callers must explicitly release connections via pool.release()
        CompletionStage<BoundedAsyncPool<StatefulRedisConnection<K, V>>> stage =
            AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                () -> CompletableFuture.supplyAsync(() -> createConnection(redisClient, config)),
                boundedPoolConfig,
                false
            );
        return stage.toCompletableFuture().join();
    }

    /**
     * Creates a pool of default Redis cluster connections.
     *
     * @param redisClient The Redis cluster client
     * @param config The Redis configuration
     * @param poolConfiguration The pool configuration
     * @return The connection pool
     */
    @Bean(preDestroy = "close")
    @Singleton
    @Primary
    @Requires(beans = RedisClusterClient.class)
    @Requires(property = RedisSetting.REDIS_URIS)
    public AsyncPool<StatefulRedisClusterConnection<K, V>> redisClusterConnectionPool(
        @Primary RedisClusterClient redisClient,
        @Primary DefaultRedisConfiguration config,
        DefaultRedisConnectionPoolConfiguration poolConfiguration
    ) {
        BoundedPoolConfig boundedPoolConfig = poolConfiguration.getBoundedPoolConfig();
        // wrapConnections=false: callers must explicitly release connections via pool.release()
        CompletionStage<BoundedAsyncPool<StatefulRedisClusterConnection<K, V>>> stage =
            AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                () -> CompletableFuture.supplyAsync(() -> createClusterConnection(redisClient, config)),
                boundedPoolConfig,
                false
            );
        return stage.toCompletableFuture().join();
    }

    /**
     * Creates a standalone or master-replica connection for the configured server definition.
     * Subclasses overriding this method must keep the default codec and preserve the existing
     * read-preference behavior for replica-aware connections.
     *
     * @param redisClient The Redis client
     * @param config The Redis configuration
     * @return The Redis connection
     */
    StatefulRedisConnection<K, V> createConnection(RedisClient redisClient, AbstractRedisConfiguration config) {
        Optional<RedisURI> redisUri = config.getUri();
        if (redisUri.isPresent() && !config.getReplicaUris().isEmpty()) {
            List<RedisURI> uris = new ArrayList<>(config.getReplicaUris());
            uris.add(redisUri.orElseThrow());

            StatefulRedisMasterReplicaConnection<K, V> connection = createMasterReplicaConnection(redisClient, uris);
            config.getReadFrom().ifPresent(connection::setReadFrom);
            return connection;
        }
        return redisClient.connect(defaultCodec);
    }

    /**
     * Creates the underlying master-replica connection used by {@link #createConnection(RedisClient, AbstractRedisConfiguration)}.
     * Subclasses overriding this method must return an open connection backed by the supplied URIs
     * and compatible with the factory's default codec.
     *
     * @param redisClient The Redis client
     * @param redisUris The ordered Redis URIs, including the primary URI
     * @return The master-replica connection
     */
    StatefulRedisMasterReplicaConnection<K, V> createMasterReplicaConnection(RedisClient redisClient, List<RedisURI> redisUris) {
        return MasterReplica.connect(redisClient, defaultCodec, redisUris);
    }

    /**
     * Creates a cluster connection for the configured Redis client.
     * Subclasses overriding this method must apply any configured read preference before returning
     * the connection.
     *
     * @param redisClient The Redis cluster client
     * @param config The Redis configuration
     * @return The Redis cluster connection
     */
    StatefulRedisClusterConnection<K, V> createClusterConnection(
        RedisClusterClient redisClient,
        AbstractRedisConfiguration config
    ) {
        StatefulRedisClusterConnection<K, V> connection = redisClient.connect(defaultCodec);
        config.getReadFrom().ifPresent(connection::setReadFrom);
        return connection;
    }
}
