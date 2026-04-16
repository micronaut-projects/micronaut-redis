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
import io.micronaut.context.BeanContext;
import io.micronaut.context.RuntimeBeanDefinition;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

/**
 * Registers additional typed top-level Redis connection beans for unqualified codecs.
 */
@Context
@Singleton
@Requires(beans = DefaultRedisConfiguration.class)
@Requires(missingProperty = RedisSetting.REDIS_URIS)
final class TopLevelRedisConnectionRegistrar {

    private final BeanContext beanContext;

    TopLevelRedisConnectionRegistrar(BeanContext beanContext) {
        this.beanContext = beanContext;
    }

    @PostConstruct
    void registerBeans() {
        for (BeanDefinition<RedisCodec> codecDefinition : beanContext.getBeanDefinitions(RedisCodec.class)) {
            if (codecDefinition.isPrimary() || codecDefinition.getBeanName().isPresent()) {
                continue;
            }
            List<Argument<?>> typeArguments = codecDefinition.getTypeArguments(RedisCodec.class);
            if (typeArguments.size() != 2) {
                continue;
            }

            registerConnection(typeArguments.get(0), typeArguments.get(1));
            registerPubSubConnection(typeArguments.get(0), typeArguments.get(1));
        }
    }

    private void registerConnection(Argument<?> keyType, Argument<?> valueType) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Argument<StatefulRedisConnection> beanType = (Argument) Argument.of(StatefulRedisConnection.class, keyType, valueType);
        beanContext.registerBeanDefinition(
            RuntimeBeanDefinition.builder(beanType, () -> createConnection(keyType, valueType))
                .singleton(true)
                .build()
        );
    }

    private void registerPubSubConnection(Argument<?> keyType, Argument<?> valueType) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Argument<StatefulRedisPubSubConnection> beanType = (Argument) Argument.of(StatefulRedisPubSubConnection.class, keyType, valueType);
        beanContext.registerBeanDefinition(
            RuntimeBeanDefinition.builder(beanType, () -> createPubSubConnection(keyType, valueType))
                .singleton(true)
                .build()
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private StatefulRedisConnection<?, ?> createConnection(Argument<?> keyType, Argument<?> valueType) {
        RedisClient redisClient = beanContext.getBean(RedisClient.class);
        AbstractRedisConfiguration config = beanContext.getBean(AbstractRedisConfiguration.class);
        RedisCodec codec = (RedisCodec) beanContext.getBean((Argument) Argument.of(RedisCodec.class, keyType, valueType));
        var primaryUri = config.getUri();
        if (primaryUri.isPresent() && !config.getReplicaUris().isEmpty()) {
            List<RedisURI> uris = new ArrayList<>(config.getReplicaUris());
            uris.add(primaryUri.get());

            StatefulRedisMasterReplicaConnection<?, ?> connection = MasterReplica.connect(
                redisClient,
                codec,
                uris
            );
            config.getReadFrom().ifPresent(connection::setReadFrom);
            return connection;
        }
        return redisClient.connect(codec);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private StatefulRedisPubSubConnection<?, ?> createPubSubConnection(Argument<?> keyType, Argument<?> valueType) {
        RedisClient redisClient = beanContext.getBean(RedisClient.class);
        RedisCodec codec = (RedisCodec) beanContext.getBean((Argument) Argument.of(RedisCodec.class, keyType, valueType));
        return redisClient.connectPubSub(codec);
    }
}
