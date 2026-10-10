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
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.BeanDependencyResolver;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Retain;
import io.micronaut.context.exceptions.NoSuchBeanException;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A factory bean for constructing {@link RedisClient} instances from {@link NamedRedisServersConfiguration} instances.
 *
 * <p>The beans it creates resolve the named {@link ClientResources}, {@link RedisCodec} and {@link RedisClient} they
 * are built from as their own dependencies, so that development mode, which keeps them across a restart, knows what
 * they hold. The factory holds no bean locator.</p>
 *
 * @author Graeme Rocher
 * @param <K> Key type
 * @param <V> Value type
 * @since 1.0
 */
@Factory
public class NamedRedisClientFactory<K, V> extends AbstractRedisClientFactory<K, V> {

    private final @Nullable BeanLocator beanLocator;
    private final @Nullable ClientResources defaultClientResources;

    /**
     * @param codec The default RedisCodec
     * @since 7.3.0
     */
    @Inject
    public NamedRedisClientFactory(@Primary RedisCodec<K, V> codec) {
        super(codec);
        this.beanLocator = null;
        this.defaultClientResources = null;
    }

    /**
     * @param beanLocator The BeanLocator
     * @param defaultClientResources The ClientResources
     * @param codec The RedisCodec
     * @deprecated The factory resolves what the beans it creates are built from as their dependencies, and holds no
     * bean locator. Use {@link #NamedRedisClientFactory(RedisCodec)}.
     */
    @Deprecated(since = "7.3.0", forRemoval = true)
    public NamedRedisClientFactory(BeanLocator beanLocator, @Primary @Nullable ClientResources defaultClientResources, @Primary RedisCodec<K, V> codec) {
        super(codec);
        this.beanLocator = beanLocator;
        this.defaultClientResources = defaultClientResources;
    }

    /**
     * Creates the {@link RedisClient} from the configuration. Development mode keeps it, with its resources, across a
     * restart, until a change under {@code redis} releases it.
     *
     * @param config The configuration
     * @param defaultClientResources The default ClientResources, used when the server has none of its name
     * @param mutators the list of mutators
     * @param dependencies Resolves the ClientResources of the server's name, as a dependency of the client
     * @return The {@link RedisClient}
     * @since 7.3.0
     */
    @Bean(preDestroy = "shutdown")
    @EachBean(NamedRedisServersConfiguration.class)
    @Retain(invalidatedBy = RedisSetting.PREFIX)
    public RedisClient redisClient(NamedRedisServersConfiguration config,
                                   @Primary @Nullable ClientResources defaultClientResources,
                                   @Nullable List<ClientResourcesMutator> mutators,
                                   BeanDependencyResolver dependencies) {
        ClientResources clientResources = find(dependencies, ClientResources.class, config.getName());
        return super.redisClient(config, clientResources != null ? clientResources : defaultClientResources, mutators);
    }

    /**
     * Creates the {@link RedisClient} from the configuration.
     *
     * @param config The configuration
     * @param mutators the list of mutators
     * @return The {@link RedisClient}
     * @deprecated Use {@link #redisClient(NamedRedisServersConfiguration, ClientResources, List, BeanDependencyResolver)},
     * which a factory created with {@link #NamedRedisClientFactory(RedisCodec)} supports.
     */
    @Deprecated(since = "7.3.0", forRemoval = true)
    public RedisClient redisClient(NamedRedisServersConfiguration config, @Nullable List<ClientResourcesMutator> mutators) {
        return super.redisClient(config, getClientResources(config), mutators);
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}, with the codec of the server's name, or
     * the default one. Development mode keeps it across a restart, until a change under {@code redis} releases it, when
     * neither the codec nor anything else it holds is of the application.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @param dependencies Resolves the client and the codec of the server's name, as dependencies of the connection
     * @return The {@link StatefulRedisConnection}
     * @since 7.3.0
     */
    @Bean(preDestroy = "close")
    @EachBean(NamedRedisServersConfiguration.class)
    @Retain(invalidatedBy = RedisSetting.PREFIX)
    public StatefulRedisConnection<K, V> redisConnection(NamedRedisServersConfiguration config, BeanDependencyResolver dependencies) {
        return redisConnection(config, codec(dependencies, config), dependencies.getBean(RedisClient.class, Qualifiers.byName(config.getName())));
    }

    /**
     * Creates the {@link StatefulRedisConnection} from the {@link RedisClient}.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @return The {@link StatefulRedisConnection}
     * @deprecated Use {@link #redisConnection(NamedRedisServersConfiguration, BeanDependencyResolver)}, which a factory
     * created with {@link #NamedRedisClientFactory(RedisCodec)} supports.
     */
    @Deprecated(since = "7.3.0", forRemoval = true)
    public StatefulRedisConnection<K, V> redisConnection(NamedRedisServersConfiguration config) {
        BeanLocator locator = locator();
        RedisCodec<K, V> namedCodec = locator.findBean(RedisCodec.class, Qualifiers.byName(config.getName())).orElse(defaultCodec);
        return redisConnection(config, namedCodec, getRedisClient(config));
    }

    private StatefulRedisConnection<K, V> redisConnection(NamedRedisServersConfiguration config, RedisCodec<K, V> namedCodec, RedisClient redisClient) {
        StatefulRedisConnection<K, V> connection;
        if (config.getUri().isPresent() && !config.getReplicaUris().isEmpty()) {
            List<RedisURI> uris = new ArrayList<>(config.getReplicaUris());
            uris.add(config.getUri().get());

            connection = MasterReplica.connect(
                redisClient,
                namedCodec,
                uris
            );
            if (config.getReadFrom().isPresent()) {
                ((StatefulRedisMasterReplicaConnection<K, V>) connection).setReadFrom(config.getReadFrom().get());
            }
        } else {
            connection = super.redisConnection(redisClient, namedCodec);

            if (connection instanceof StatefulRedisClusterConnection<?, ?> conn && config.getReadFrom().isPresent()) {
                conn.setReadFrom(config.getReadFrom().get());
            }
        }

        return connection;
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}, with the codec of the server's
     * name, or the default one. Development mode keeps it across a restart, until a change under {@code redis} releases
     * it, when neither the codec nor anything else it holds is of the application; the listeners and subscriptions of
     * the stopped application are removed from it as it stops.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @param dependencies Resolves the client and the codec of the server's name, as dependencies of the connection
     * @return The {@link StatefulRedisPubSubConnection}
     * @since 7.3.0
     */
    @Bean(preDestroy = "close")
    @EachBean(NamedRedisServersConfiguration.class)
    @Retain(invalidatedBy = RedisSetting.PREFIX)
    public StatefulRedisPubSubConnection<K, V> redisPubSubConnection(NamedRedisServersConfiguration config, BeanDependencyResolver dependencies) {
        return super.redisPubSubConnection(dependencies.getBean(RedisClient.class, Qualifiers.byName(config.getName())), codec(dependencies, config));
    }

    /**
     * Creates the {@link StatefulRedisPubSubConnection} from the {@link RedisClient}.
     *
     * @param config The {@link NamedRedisServersConfiguration}
     * @return The {@link StatefulRedisPubSubConnection}
     * @deprecated Use {@link #redisPubSubConnection(NamedRedisServersConfiguration, BeanDependencyResolver)}, which a
     * factory created with {@link #NamedRedisClientFactory(RedisCodec)} supports.
     */
    @Deprecated(since = "7.3.0", forRemoval = true)
    public StatefulRedisPubSubConnection<K, V> redisPubSubConnection(NamedRedisServersConfiguration config) {
        RedisCodec<K, V> namedCodec = locator().findBean(RedisCodec.class, Qualifiers.byName(config.getName())).orElse(defaultCodec);
        return super.redisPubSubConnection(getRedisClient(config), namedCodec);
    }

    @SuppressWarnings("unchecked")
    private RedisCodec<K, V> codec(BeanDependencyResolver dependencies, NamedRedisServersConfiguration config) {
        RedisCodec<K, V> namedCodec = find(dependencies, RedisCodec.class, config.getName());
        return namedCodec != null ? namedCodec : defaultCodec;
    }

    private static <T> @Nullable T find(BeanDependencyResolver dependencies, Class<T> type, String name) {
        try {
            return dependencies.getBean(type, Qualifiers.byName(name));
        } catch (NoSuchBeanException e) {
            return null;
        }
    }

    private BeanLocator locator() {
        if (beanLocator == null) {
            throw new IllegalStateException("A NamedRedisClientFactory created without a bean locator creates its beans with a BeanDependencyResolver");
        }
        return beanLocator;
    }

    /**
     * Finds named {@link ClientResources} or uses default if exists.
     * @param config named config
     * @return named The ClientResources
     */
    private @Nullable ClientResources getClientResources(NamedRedisServersConfiguration config) {
        return locator().findBean(ClientResources.class, Qualifiers.byName(config.getName())).orElse(this.defaultClientResources);
    }

    /**
     * Finds named {@link RedisClient}.
     * @param config named config
     * @return named The RedisClient
     */
    private RedisClient getRedisClient(NamedRedisServersConfiguration config) {
        return locator().getBean(RedisClient.class, Qualifiers.byName(config.getName()));
    }

}
