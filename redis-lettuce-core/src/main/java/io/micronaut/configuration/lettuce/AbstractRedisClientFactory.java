/*
 * Copyright 2017-2023 original authors
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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.env.DevelopmentActive;
import jakarta.inject.Inject;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Abstract version of a factory class for creating Redis clients.
 *
 * @author Graeme Rocher
 * @param <K> Key type
 * @param <V> Value type
 * @since 1.0
 */
public abstract class AbstractRedisClientFactory<K, V> {

    protected final RedisCodec<K, V> defaultCodec;

    private @Nullable ResourceOwningClients resourceOwningClients;

    /**
     * @param defaultCodec The default codec
     */
    protected AbstractRedisClientFactory(RedisCodec<K, V> defaultCodec) {
        this.defaultCodec = defaultCodec;
    }

    /**
     * Receives the clients of {@link DevelopmentActive development mode}, a bean that is present in development mode
     * only and holds no state, so that the factory, and the clients development mode keeps across a restart, are not
     * bound to the context.
     *
     * @param resourceOwningClients The clients of development mode, null outside it
     */
    @Inject
    void resourceOwningClients(@Nullable ResourceOwningClients resourceOwningClients) {
        this.resourceOwningClients = resourceOwningClients;
    }

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
     * Creates the {@link RedisClient} from the configuration, on {@link ClientResources} built for it from the given
     * ones and the mutators. In development mode, which keeps the client across a restart, the client owns the
     * resources it is built on, and shuts them down with itself, but for what they share with the given resources,
     * and it copies the URI it connects to rather than keep the configuration.
     *
     * @param config The configuration
     * @param optionalClientResources The ClientResources
     * @param mutators The list of mutators
     * @return The {@link RedisClient}
     */
    public RedisClient redisClient(AbstractRedisConfiguration config,
                                   @Nullable ClientResources optionalClientResources,
                                   @Nullable List<ClientResourcesMutator> mutators) {
        ClientResources clientResources = configureClientResources(config, optionalClientResources, mutators);
        if (clientResources == null) {
            return redisClient(config);
        }
        Optional<RedisURI> uri = config.getUri();
        if (resourceOwningClients != null) {
            return resourceOwningClients.redisClient(clientResources, uri.orElse(config));
        }
        return uri.map(redisURI -> RedisClient.create(clientResources, redisURI))
            .orElseGet(() -> RedisClient.create(clientResources, config));
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}.
     *
     * @param redisClient The {@link RedisClient}
     * @param codec The codec to use
     * @return The {@link StatefulRedisConnection}
     */
    public StatefulRedisConnection<K, V> redisConnection(RedisClient redisClient, RedisCodec<K, V> codec) {
        return redisClient.connect(codec);
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}.
     *
     * @param redisClient The {@link RedisClient}
     * @param codec The codec to use
     * @return The {@link StatefulRedisPubSubConnection}
     */
    public StatefulRedisPubSubConnection<K, V> redisPubSubConnection(RedisClient redisClient, RedisCodec<K, V> codec) {
        return redisClient.connectPubSub(codec);
    }

    @Nullable
    private ClientResources configureClientResources(AbstractRedisConfiguration config, @Nullable ClientResources clientResources, @Nullable List<ClientResourcesMutator> mutators) {
        ClientResources.Builder clientResourcesBuilder = clientResources == null ?  ClientResources.builder() : clientResources.mutate();
        if (mutators != null) {
            mutators.forEach(clientResourcesMutator -> clientResourcesMutator.mutate(clientResourcesBuilder, config));
        }
        return resourceOwningClients != null ? resourceOwningClients.build(clientResourcesBuilder) : clientResourcesBuilder.build();
    }
}
