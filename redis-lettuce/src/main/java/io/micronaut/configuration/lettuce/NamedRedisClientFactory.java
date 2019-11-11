/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;

/**
 * A factory bean for constructing {@link RedisClient} instances from {@link NamedRedisServersConfiguration} instances.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Factory
public class NamedRedisClientFactory extends AbstractRedisClientFactory {

    /**
     * Creates the {@link RedisClient} from the configuration.
     *
     * @param config The configuration
     * @return The {@link RedisClient}
     */
    @Bean(preDestroy = "shutdown")
    @EachBean(NamedRedisServersConfiguration.class)
    public RedisClient redisClient(NamedRedisServersConfiguration config) {
        return super.redisClient(config);
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @return The {@link StatefulRedisConnection}
     */
    @Bean(preDestroy = "close")
    @EachBean(NamedRedisServersConfiguration.class)
    public StatefulRedisConnection<String, String> redisConnection(NamedRedisServersConfiguration config) {
        return super.redisConnection(super.redisClient(config));
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @return The {@link StatefulRedisPubSubConnection}
     */
    @Bean(preDestroy = "close")
    @EachBean(NamedRedisServersConfiguration.class)
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(NamedRedisServersConfiguration config) {
        return super.redisPubSubConnection(super.redisClient(config));
    }
}
