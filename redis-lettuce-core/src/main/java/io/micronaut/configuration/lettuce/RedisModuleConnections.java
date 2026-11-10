/*
 * Copyright 2017-2026 original authors
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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Retain;
import io.micronaut.core.annotation.Internal;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The byte array connections that the Redis modules open on the clients for themselves: the connection of a cache,
 * of the session store, of the Pub/Sub publisher and of the Pub/Sub listeners. A module asks for its connection
 * by a key of its own as it is created, and this bean opens it once, on the client of the server, and closes it as
 * it is destroyed itself, with the context.
 *
 * <p>It holds the connections only, and nothing of the module that asked for them, so development mode keeps it
 * across a restart, until a change under {@code redis} releases it: the next generation of a module asks again
 * and receives the same connection, as long as the client it is opened on is the same, which development mode
 * keeps too. A module that registers listeners or subscriptions on its connection removes them as it is destroyed.
 * A connection whose client went, such as a client that could not be kept, is closed with it, and opened again on
 * the new one.</p>
 *
 * @author graemerocher
 * @since 7.3.0
 */
@Internal
@Singleton
@Retain(invalidatedBy = RedisSetting.PREFIX)
public final class RedisModuleConnections implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RedisModuleConnections.class);

    private final Map<String, OpenConnection> connections = new ConcurrentHashMap<>();

    /**
     * The byte array connection of a key, on the client of the named server or the default one.
     *
     * @param key The key of the module that uses the connection
     * @param beanLocator The bean locator of the module that asks, to find the client with
     * @param serverName The server name, or empty for the default server
     * @param errorMessage The message of the exception thrown when there is no client
     * @return The connection
     */
    public StatefulConnection<byte[], byte[]> connection(String key, BeanLocator beanLocator, Optional<String> serverName, String errorMessage) {
        AbstractRedisClient client = RedisConnectionUtil.findClient(beanLocator, serverName, errorMessage);
        return connection("connection:" + key, client, () -> RedisConnectionUtil.openBytesRedisConnection(beanLocator, client));
    }

    /**
     * The byte array Pub/Sub connection of a key, on the client of the named server or the default one.
     *
     * @param key The key of the module that uses the connection
     * @param beanLocator The bean locator of the module that asks, to find the client with
     * @param serverName The server name, or empty for the default server
     * @param errorMessage The message of the exception thrown when there is no client
     * @return The connection
     */
    public StatefulRedisPubSubConnection<byte[], byte[]> pubSubConnection(String key, BeanLocator beanLocator, Optional<String> serverName, String errorMessage) {
        AbstractRedisClient client = RedisConnectionUtil.findClient(beanLocator, serverName, errorMessage);
        return connection("pubsub:" + key, client, () -> RedisConnectionUtil.openBytesRedisPubSubConnection(client));
    }

    @SuppressWarnings("unchecked")
    private <C extends StatefulConnection<byte[], byte[]>> C connection(String key, AbstractRedisClient client, Supplier<C> opener) {
        // a connection the client of which went is closed with it: those of other keys are dropped too, so that
        // nothing of a client that was shut down stays reachable
        connections.values().removeIf(open -> !open.connection.isOpen());
        OpenConnection open = connections.compute(key, (ignored, existing) -> {
            if (existing != null && existing.client == client && existing.connection.isOpen()) {
                return existing;
            }
            if (existing != null) {
                closeQuietly(existing.connection);
            }
            return new OpenConnection(client, opener.get());
        });
        return (C) open.connection;
    }

    /**
     * Drops the connections that were closed, such as those of a client that was shut down. Development mode calls it
     * on the bean it kept across a restart, once the clients that were not kept are shut down.
     */
    public void releaseClosed() {
        connections.values().removeIf(open -> !open.connection.isOpen());
    }

    @PreDestroy
    @Override
    public void close() {
        for (OpenConnection open : connections.values()) {
            closeQuietly(open.connection);
        }
        connections.clear();
    }

    private static void closeQuietly(StatefulConnection<?, ?> connection) {
        if (!connection.isOpen()) {
            // closed with its client
            return;
        }
        try {
            connection.close();
        } catch (RuntimeException e) {
            LOG.debug("Failed to close the Redis connection {}", connection, e);
        }
    }

    /**
     * A connection, and the client it was opened on.
     *
     * @param client The client
     * @param connection The connection
     */
    private record OpenConnection(AbstractRedisClient client, StatefulConnection<byte[], byte[]> connection) {
    }
}
