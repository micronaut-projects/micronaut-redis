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
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.micronaut.configuration.lettuce.DefaultRedisConfiguration;
import io.micronaut.configuration.lettuce.DefaultRedisConnectionPoolConfiguration;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.ConfigurationException;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Default redis connection pool factory.
 *
 * @author Kovalov Illia
 * @since 5.3.0
 */
@Factory
public final class RedisAsyncConnectionPoolFactory {

    @Singleton
    @Requires(beans = {DefaultRedisCacheConfiguration.class, DefaultRedisConnectionPoolConfiguration.class, DefaultRedisConfiguration.class})
    public AsyncPool<StatefulConnection<byte[], byte[]>> getAsyncPool(
            DefaultRedisCacheConfiguration defaultRedisCacheConfiguration,
            DefaultRedisConfiguration defaultRedisConfiguration,
            BeanLocator beanLocator,
            DefaultRedisConnectionPoolConfiguration defaultRedisConnectionPoolConfiguration
    ) {
        Optional<String> server = defaultRedisCacheConfiguration.getServer();
        AbstractRedisClient client = RedisConnectionUtil.findClient(beanLocator, server, "No Redis server configured to allow caching");
        BoundedPoolConfig asyncConfig = defaultRedisConnectionPoolConfiguration.getBoundedPoolConfig();
        CompletionStage<BoundedAsyncPool<StatefulConnection<byte[], byte[]>>> stage =  AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(() -> {
                    if (client instanceof RedisClusterClient) {
                        StatefulRedisClusterConnection<byte[], byte[]> connection = ((RedisClusterClient) client).connect(new ByteArrayCodec());

                        if (defaultRedisConfiguration.getReadFrom().isPresent()) {
                            connection.setReadFrom(defaultRedisConfiguration.getReadFrom().get());
                        }

                        return CompletableFuture.completedFuture(connection);
                    }
                    if (client instanceof RedisClient) {
                        if (defaultRedisConfiguration.getUri().isPresent() && !defaultRedisConfiguration.getReplicaUris().isEmpty()) {
                            List<RedisURI> uris = new ArrayList<>(defaultRedisConfiguration.getReplicaUris());
                            uris.add(defaultRedisConfiguration.getUri().get());

                            StatefulRedisMasterReplicaConnection<byte[], byte[]> connection = MasterReplica.connect(
                                (RedisClient) client,
                                new ByteArrayCodec(),
                                uris
                            );
                            if (defaultRedisConfiguration.getReadFrom().isPresent()) {
                                connection.setReadFrom(defaultRedisConfiguration.getReadFrom().get());
                            }

                            return CompletableFuture.completedFuture(connection);
                        } else {
                            return CompletableFuture.completedFuture(((RedisClient) client).connect(new ByteArrayCodec()));
                        }
                    }
                    throw new ConfigurationException("Invalid Redis connection");
                },
                asyncConfig
        );
        return stage.toCompletableFuture().join();
    }
}
