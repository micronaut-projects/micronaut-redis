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
package io.micronaut.configuration.lettuce.pubsub;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micronaut.configuration.lettuce.AbstractRedisConfiguration;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.context.BeanLocator;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.type.Argument;
import io.micronaut.http.MediaType;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes messages to Redis Pub/Sub channels.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
@Requires(beans = AbstractRedisConfiguration.class)
public class RedisPubSubPublisher implements AutoCloseable {

    private static final String DEFAULT_CONNECTION = "<default>";

    private final BeanLocator beanLocator;
    private final RedisMessageBodyHandler messageBodyHandler;
    private final Map<String, Object> connections = new ConcurrentHashMap<>();

    /**
     * @param beanLocator        The bean locator
     * @param messageBodyHandler The message body handler
     */
    public RedisPubSubPublisher(BeanLocator beanLocator, RedisMessageBodyHandler messageBodyHandler) {
        this.beanLocator = beanLocator;
        this.messageBodyHandler = messageBodyHandler;
    }

    /**
     * Publish a message to the default Redis connection.
     *
     * @param channel The channel name
     * @param body    The message body
     * @return The Redis publish subscriber count
     */
    public long publish(String channel, Object body) {
        return publishOnConnection(null, channel, body);
    }

    /**
     * Publish a message to a named Redis connection.
     *
     * @param connectionName The named connection
     * @param channel        The channel name
     * @param body           The message body
     * @return The Redis publish subscriber count
     */
    public long publishOnConnection(@Nullable String connectionName, String channel, Object body) {
        return publishOnConnection(
            connectionName,
            channel,
            Argument.of(body == null ? Object.class : body.getClass()),
            AnnotationMetadata.EMPTY_METADATA,
            body
        );
    }

    /**
     * Publish a message to a named Redis connection.
     *
     * @param connectionName    The named connection
     * @param channel           The channel name
     * @param argument          The body argument
     * @param annotationMetadata The body annotation metadata
     * @param body              The message body
     * @return The Redis publish subscriber count
     */
    public long publishOnConnection(@Nullable String connectionName,
                                    String channel,
                                    Argument<?> argument,
                                    AnnotationMetadata annotationMetadata,
                                    Object body) {
        return publishOnConnection(connectionName, channel, argument, messageBodyHandler.resolveOutgoingMediaType(annotationMetadata, AnnotationMetadata.EMPTY_METADATA), body);
    }

    /**
     * Publish a message using an explicit media type.
     *
     * @param connectionName The named connection
     * @param channel        The channel name
     * @param argument       The body argument
     * @param mediaType      The media type
     * @param body           The message body
     * @return The Redis publish subscriber count
     */
    public long publishOnConnection(@Nullable String connectionName,
                                    String channel,
                                    Argument<?> argument,
                                    MediaType mediaType,
                                    Object body) {
        Object connection = connections.computeIfAbsent(connectionName == null ? DEFAULT_CONNECTION : connectionName, ignored ->
            RedisConnectionUtil.openBytesRedisConnection(
                beanLocator,
                Optional.ofNullable(connectionName),
                "No Redis server configured for Pub/Sub publishing."
            )
        );
        byte[] channelBytes = channel.getBytes(StandardCharsets.UTF_8);
        byte[] bodyBytes = messageBodyHandler.serialize(argument, mediaType, body);
        if (connection instanceof StatefulRedisConnection redisConnection) {
            @SuppressWarnings("unchecked")
            StatefulRedisConnection<byte[], byte[]> typedConnection = (StatefulRedisConnection<byte[], byte[]>) redisConnection;
            return typedConnection.sync().publish(channelBytes, bodyBytes);
        }
        if (connection instanceof StatefulRedisClusterConnection redisClusterConnection) {
            @SuppressWarnings("unchecked")
            StatefulRedisClusterConnection<byte[], byte[]> typedConnection = (StatefulRedisClusterConnection<byte[], byte[]>) redisClusterConnection;
            return typedConnection.sync().publish(channelBytes, bodyBytes);
        }
        throw new IllegalStateException("Unsupported Redis connection type [" + connection.getClass().getName() + "]");
    }

    @PreDestroy
    @Override
    public void close() {
        connections.values().forEach(connection -> {
            if (connection instanceof AutoCloseable autoCloseable) {
                try {
                    autoCloseable.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        });
        connections.clear();
    }
}
