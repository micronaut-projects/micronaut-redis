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
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;

import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Factory for the default {@link RedisClient}. Creates the injectable {@link Primary} bean.
 *
 * @author Graeme Rocher
 * @param <K> Key type
 * @param <V> Value type
 * @since 1.0
 */
@Requires(beans = DefaultRedisConfiguration.class)
@Singleton
@Factory
@Requires(missingProperty = RedisSetting.REDIS_URIS)
public class DefaultRedisClientFactory<K, V> extends AbstractRedisClientFactory<K, V> {

    private final BeanLocator beanLocator;

    public DefaultRedisClientFactory(@Primary RedisCodec<K, V> codec, BeanLocator beanLocator) {
        super(codec);
        this.beanLocator = beanLocator;
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Primary
    @Override
    public RedisClient redisClient(@Primary AbstractRedisConfiguration config, @Nullable @Primary ClientResources defaultClientResources, @Nullable List<ClientResourcesMutator> mutators) {
        return super.redisClient(config, defaultClientResources, mutators);
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}.
     *
     * @param redisClient The {@link RedisClient}
     * @return The {@link StatefulRedisConnection}
     */
    @Bean(preDestroy = "close")
    @Singleton
    @Primary
    public StatefulRedisConnection<K, V> redisConnection(@Primary RedisClient redisClient) {
        Optional<AbstractRedisConfiguration> config = beanLocator.findBean(AbstractRedisConfiguration.class);

        if (config.isPresent() && config.get().getReplicaUris().size() > 0) {
            List<RedisURI> uris = new ArrayList(config.get().getReplicaUris());
            uris.add(config.get().getUri().get());

            StatefulRedisMasterReplicaConnection connection = MasterReplica.connect(
                redisClient,
                defaultCodec,
                uris
            );
            if (config.get().getReadFrom() != null) {
                connection.setReadFrom(config.get().getReadFrom());
            }

            return connection;
        } else {
            return super.redisConnection(redisClient, defaultCodec);
        }
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}.
     *
     * @param redisClient The {@link RedisClient}
     * @return The {@link StatefulRedisPubSubConnection}
     */
    @Bean(preDestroy = "close")
    @Singleton
    public StatefulRedisPubSubConnection<K, V> redisPubSubConnection(@Primary RedisClient redisClient) {
        return super.redisPubSubConnection(redisClient, defaultCodec);
    }
}
