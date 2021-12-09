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
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.CollectionUtils;
import java.util.Collections;
import java.util.List;

/**
 * Abstract version of the a factory class for creating Redis clients.
 *
 */
public abstract class AbstractRedisClusterClientFactory {
    /**
     * Create the client based on config URIs.
     * @param config config
     * @param defaultClientResources default {@link ClientResources}
     * @deprecated use {@link #redisClient(AbstractRedisConfiguration, ClientResources, List)} instead
     * @return client
     */
    @Deprecated
    public RedisClusterClient redisClient(AbstractRedisConfiguration config, @Nullable ClientResources defaultClientResources) {
        return this.redisClient(config, defaultClientResources, Collections.emptyList());
    }

    /**
     * Create the client based on config URIs.
     * @param config config
     * @param optionalClientResources default {@link ClientResources}
     * @param mutators The list of mutators
     * @return client
     */
    public RedisClusterClient redisClient(AbstractRedisConfiguration config,
                                          @Nullable ClientResources optionalClientResources,
                                          @Nullable List<ClientResourcesMutator> mutators) {
        ClientResources clientResources = configureClientResources(config, optionalClientResources, mutators);
        List<RedisURI> uris = config.getUris();
        if (CollectionUtils.isEmpty(uris)) {
            throw new ConfigurationException("Redis URIs must be specified");
        }
        return clientResources == null ? RedisClusterClient.create(uris) : RedisClusterClient.create(clientResources, uris);
    }

    /**
     * Establish redis connection.
     * @param redisClient client.
     * @return connection
     */
    public StatefulRedisClusterConnection<String, String> redisConnection(RedisClusterClient redisClient) {
        return redisClient.connect();
    }

    /**
     *
     * @param redisClient redisClient
     * @return connection
     */
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(RedisClusterClient redisClient) {
        return redisClient.connectPubSub();
    }

    @Nullable
    private ClientResources configureClientResources(AbstractRedisConfiguration config, @Nullable ClientResources clientResources, @Nullable List<ClientResourcesMutator> mutators) {
        ClientResources.Builder clientResourcesBuilder = clientResources == null ?  ClientResources.builder() : clientResources.mutate();
        if (mutators != null) {
            mutators.forEach(clientResourcesMutator -> clientResourcesMutator.mutate(clientResourcesBuilder, config));
        }
        return clientResourcesBuilder.build();
    }
}
