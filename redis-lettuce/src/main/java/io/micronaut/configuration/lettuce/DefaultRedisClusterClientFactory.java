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
package io.micronaut.configuration.lettuce;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.util.CollectionUtils;

import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Allows connecting to a Redis cluster via the {@code "redis.uris"} setting.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Requires(property = RedisSetting.REDIS_URIS)
@Singleton
@Factory
public class DefaultRedisClusterClientFactory {

    /**
     * Create the client based on config URIs.
     * @param config config
     * @param defaultClientResources default {@link ClientResources}
     * @deprecated use {@link #redisClient(AbstractRedisConfiguration, ClientResources, List)} instead
     * @return client
     */
    @Deprecated(since = "6.1.0", forRemoval = true)
    public RedisClusterClient redisClient(AbstractRedisConfiguration config, @Nullable ClientResources defaultClientResources) {
        return this.redisClient(config, defaultClientResources, Collections.emptyList());
    }

    /**
     * Create the client based on config URIs and optional client resource mutators.
     * @param config config
     * @param defaultClientResources default {@link ClientResources}
     * @param mutators The list of mutators
     * @return client
     * @since 6.1.0
     */
    @Bean(preDestroy = "shutdown")
    @Singleton
    @Primary
    public RedisClusterClient redisClient(@Primary AbstractRedisConfiguration config,
                                          @Primary @Nullable ClientResources defaultClientResources,
                                          @Nullable List<ClientResourcesMutator> mutators) {
        List<RedisURI> uris = config.getUris();
        if (CollectionUtils.isEmpty(uris)) {
            throw new ConfigurationException("Redis URIs must be specified");
        }
        final ClientResources.Builder builder = Optional.ofNullable(defaultClientResources)
            .map(ClientResources::mutate)
            .orElseGet(ClientResources::builder);
        if (mutators != null) {
            mutators.forEach(m -> m.mutate(builder, config));
        }
        return RedisClusterClient.create(builder.build(), uris);
    }

    /**
     * Establish redis connection.
     * @param redisClient client.
     * @param config config.
     * @return connection
     * @since 6.5.0
     */
    @Bean(preDestroy = "close")
    @Singleton
    @Primary
    public StatefulRedisClusterConnection<String, String> redisConnection(@Primary RedisClusterClient redisClient, @Primary AbstractRedisConfiguration config) {
        StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
        if (config.getReadFrom() != null) {
            connection.setReadFrom(config.getReadFrom());
        }
        return connection;
    }

    /**
     * Establish redis connection.
     * @param redisClient client.
     * @return connection
     * @deprecated use {@link #redisConnection(RedisClusterClient, AbstractRedisConfiguration)} instead
     */
    @Bean(preDestroy = "close")
    @Singleton
    @Deprecated(since = "6.5.0", forRemoval = true)
    public StatefulRedisClusterConnection<String, String> redisConnection(@Primary RedisClusterClient redisClient) {
        return redisClient.connect();
    }

    /**
     *
     * @param redisClient redisClient
     * @return connection
     */
    @Bean(preDestroy = "close")
    @Singleton
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(@Primary RedisClusterClient redisClient) {
        return redisClient.connectPubSub();
    }
}
