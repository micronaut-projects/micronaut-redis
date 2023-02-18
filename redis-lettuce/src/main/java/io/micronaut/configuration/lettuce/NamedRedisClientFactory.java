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
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.inject.qualifiers.Qualifiers;


import java.util.List;

/**
 * A factory bean for constructing {@link RedisClient} instances from {@link NamedRedisServersConfiguration} instances.
 *
 * @author Graeme Rocher
 * @param <K> Key type
 * @param <V> Value type
 * @since 1.0
 */
@Factory
public class NamedRedisClientFactory<K, V> extends AbstractRedisClientFactory<K, V> {

    private final BeanLocator beanLocator;
    private final ClientResources defaultClientResources;

    /**
     * @param beanLocator The BeanLocator
     * @param defaultClientResources The ClientResources
     * @param codec The RedisCodec
     */
    public NamedRedisClientFactory(BeanLocator beanLocator, @Primary @Nullable ClientResources defaultClientResources, @Primary RedisCodec<K, V> codec) {
        super(codec);
        this.beanLocator = beanLocator;
        this.defaultClientResources = defaultClientResources;
    }

    /**
     * Creates the {@link RedisClient} from the configuration.
     *
     * @param config The configuration
     * @param mutators the list of mutators
     * @return The {@link RedisClient}
     */
    @Bean(preDestroy = "shutdown")
    @EachBean(NamedRedisServersConfiguration.class)
    public RedisClient redisClient(NamedRedisServersConfiguration config, @Nullable List<ClientResourcesMutator> mutators) {
        return super.redisClient(config, getClientResources(config), mutators);
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @return The {@link StatefulRedisConnection}
     */
    @Bean(preDestroy = "close")
    @EachBean(NamedRedisServersConfiguration.class)
    public StatefulRedisConnection<K, V> redisConnection(NamedRedisServersConfiguration config) {
        RedisCodec<K, V> namedCodec = beanLocator.findBean(RedisCodec.class, Qualifiers.byName(config.getName())).orElse(defaultCodec);
        return super.redisConnection(getRedisClient(config), namedCodec);
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @return The {@link StatefulRedisPubSubConnection}
     */
    @Bean(preDestroy = "close")
    @EachBean(NamedRedisServersConfiguration.class)
    public StatefulRedisPubSubConnection<K, V> redisPubSubConnection(NamedRedisServersConfiguration config) {
        RedisCodec<K, V> namedCodec = beanLocator.findBean(RedisCodec.class, Qualifiers.byName(config.getName())).orElse(defaultCodec);
        return super.redisPubSubConnection(getRedisClient(config), namedCodec);
    }

    /**
     * Finds named {@link ClientResources} or uses default if exists.
     * @param config named config
     * @return named The ClientResources
     */
    private @Nullable ClientResources getClientResources(NamedRedisServersConfiguration config) {
        return beanLocator.findBean(ClientResources.class, Qualifiers.byName(config.getName())).orElse(this.defaultClientResources);
    }

    /**
     * Finds named {@link RedisClient}.
     * @param config named config
     * @return named The RedisClient
     */
    private RedisClient getRedisClient(NamedRedisServersConfiguration config) {
        return beanLocator.getBean(RedisClient.class, Qualifiers.byName(config.getName()));
    }

}
