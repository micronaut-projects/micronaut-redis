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

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;

import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.List;

/**
 * Allows connecting to a Redis cluster via the the {@code "redis.uris"} setting.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Requires(property = RedisSetting.REDIS_URIS)
@Singleton
@Factory
public class DefaultRedisClusterClientFactory extends AbstractRedisClusterClientFactory {

    @Override
    @Bean(preDestroy = "shutdown")
    @Singleton
    @Primary
    public RedisClusterClient redisClient(@Primary AbstractRedisConfiguration config,
                                          @Primary @Nullable ClientResources defaultClientResources,
                                          @Nullable List<ClientResourcesMutator> mutators) {
        return super.redisClient(config, defaultClientResources, mutators);
    }

    @Override
    @Bean(preDestroy = "close")
    @Singleton
    @Primary
    public StatefulRedisClusterConnection<String, String> redisConnection(@Primary RedisClusterClient redisClient) {
        return super.redisConnection(redisClient);
    }

    @Override
    @Bean(preDestroy = "close")
    @Singleton
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(@Primary RedisClusterClient redisClient) {
        return super.redisPubSubConnection(redisClient);
    }
}
