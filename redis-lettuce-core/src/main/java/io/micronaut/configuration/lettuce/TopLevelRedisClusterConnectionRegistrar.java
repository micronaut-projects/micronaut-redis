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

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.micronaut.context.BeanContext;
import io.micronaut.context.RuntimeBeanDefinition;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registers additional typed top-level Redis cluster connection beans for unqualified codecs.
 */
@Context
@Singleton
@Requires(property = RedisSetting.REDIS_URIS)
final class TopLevelRedisClusterConnectionRegistrar {

    private final BeanContext beanContext;
    private final List<StatefulConnection<?, ?>> createdConnections = new CopyOnWriteArrayList<>();

    TopLevelRedisClusterConnectionRegistrar(BeanContext beanContext) {
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
        Argument<StatefulRedisClusterConnection> beanType = (Argument) Argument.of(StatefulRedisClusterConnection.class, keyType, valueType);
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
    private StatefulRedisClusterConnection<?, ?> createConnection(Argument<?> keyType, Argument<?> valueType) {
        RedisClusterClient redisClient = beanContext.getBean(RedisClusterClient.class);
        AbstractRedisConfiguration config = beanContext.getBean(AbstractRedisConfiguration.class);
        RedisCodec codec = (RedisCodec) beanContext.getBean((Argument) Argument.of(RedisCodec.class, keyType, valueType));
        StatefulRedisClusterConnection<?, ?> connection = redisClient.connect(codec);
        config.getReadFrom().ifPresent(connection::setReadFrom);
        createdConnections.add(connection);
        return connection;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private StatefulRedisPubSubConnection<?, ?> createPubSubConnection(Argument<?> keyType, Argument<?> valueType) {
        RedisClusterClient redisClient = beanContext.getBean(RedisClusterClient.class);
        RedisCodec codec = (RedisCodec) beanContext.getBean((Argument) Argument.of(RedisCodec.class, keyType, valueType));
        StatefulRedisPubSubConnection<?, ?> connection = redisClient.connectPubSub(codec);
        createdConnections.add(connection);
        return connection;
    }

    @PreDestroy
    void closeConnections() {
        for (StatefulConnection<?, ?> connection : createdConnections) {
            connection.close();
        }
        createdConnections.clear();
    }
}
