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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;

import io.micronaut.core.annotation.Nullable;
import java.util.Optional;

/**
 * Abstract version of the a factory class for creating Redis clients.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class AbstractRedisClientFactory {
    /**
     * Creates the {@link RedisClient} from the configuration.
     *
     * @param config The configuration
     * @return The {@link RedisClient}
     */
    public RedisClient redisClient(AbstractRedisConfiguration config) {
        Optional<RedisURI> uri = config.getUri();
        return uri.map(RedisClient::create)
            .orElseGet(() -> RedisClient.create(config));
    }

    /**
     * Creates the {@link RedisClient} from the configuration.
     *
     * @param config The configuration
     * @param optionalClientResources The ClientResources
     * @return The {@link RedisClient}
     */
    public RedisClient redisClient(AbstractRedisConfiguration config, @Nullable ClientResources optionalClientResources) {
        ClientResources clientResources = configureClientResources(config, optionalClientResources);
        if (clientResources == null) {
            return redisClient(config);
        }
        Optional<RedisURI> uri = config.getUri();
        return uri.map(redisURI -> RedisClient.create(clientResources, redisURI))
            .orElseGet(() -> RedisClient.create(clientResources, config));
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}.
     *
     * @param redisClient The {@link RedisClient}
     * @return The {@link StatefulRedisConnection}
     */
    public StatefulRedisConnection<String, String> redisConnection(RedisClient redisClient) {
        return redisClient.connect();
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}.
     *
     * @param redisClient The {@link RedisClient}
     * @return The {@link StatefulRedisPubSubConnection}
     */
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(RedisClient redisClient) {
        return redisClient.connectPubSub();
    }

    @Nullable
    private ClientResources configureClientResources(AbstractRedisConfiguration config, @Nullable ClientResources clientResources) {
        if (config.getIoThreadPoolSize() != null || config.getComputationThreadPoolSize() != null) {
            ClientResources.Builder clientResourcesBuilder = clientResources == null ?  ClientResources.builder() : clientResources.mutate();
            if (config.getIoThreadPoolSize() != null) {
                clientResourcesBuilder.ioThreadPoolSize(config.getIoThreadPoolSize());
            }
            if (config.getComputationThreadPoolSize() != null) {
                clientResourcesBuilder.computationThreadPoolSize(config.getComputationThreadPoolSize());
            }
            return clientResourcesBuilder.build();
        }
        return clientResources;
    }
}
