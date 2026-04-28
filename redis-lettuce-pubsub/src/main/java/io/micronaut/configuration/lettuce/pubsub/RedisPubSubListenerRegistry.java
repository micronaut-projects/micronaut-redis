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
package io.micronaut.configuration.lettuce.pubsub;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.micronaut.configuration.lettuce.AbstractRedisConfiguration;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.graceful.GracefulShutdownCapable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Manages Redis Pub/Sub listener registrations.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
@Requires(beans = AbstractRedisConfiguration.class)
public class RedisPubSubListenerRegistry implements AutoCloseable, GracefulShutdownCapable {

    private static final Logger LOG = LoggerFactory.getLogger(RedisPubSubListenerRegistry.class);
    private static final String DEFAULT_CONNECTION = "<default>";

    private final BeanLocator beanLocator;
    private final Map<String, ManagedConnection> managedConnections = new ConcurrentHashMap<>();
    private final AtomicLong activeTasks = new AtomicLong();
    private final AtomicBoolean gracefulShutdown = new AtomicBoolean();
    private final CompletableFuture<Void> shutdownCompletion = new CompletableFuture<>();

    /**
     * @param beanLocator The bean locator
     */
    public RedisPubSubListenerRegistry(BeanLocator beanLocator) {
        this.beanLocator = beanLocator;
    }

    /**
     * Register a listener handler for the given channel subscriptions.
     *
     * @param connectionName The optional connection name
     * @param subscriptions  The subscriptions
     * @param executor       The executor to invoke the listener on
     * @param consumer       The listener callback
     */
    public void subscribe(@Nullable String connectionName,
                          Set<ChannelSubscription> subscriptions,
                          ExecutorService executor,
                          Consumer<RedisMessage> consumer) {
        if (gracefulShutdown.get()) {
            throw new IllegalStateException("Redis Pub/Sub listeners are shutting down");
        }
        String key = connectionName == null ? DEFAULT_CONNECTION : connectionName;
        ManagedConnection managedConnection = managedConnections.computeIfAbsent(key, ignored ->
            new ManagedConnection(Optional.ofNullable(connectionName))
        );
        managedConnection.subscribe(subscriptions, executor, consumer);
    }

    @Override
    public CompletionStage<?> shutdownGracefully() {
        if (gracefulShutdown.compareAndSet(false, true)) {
            managedConnections.values().forEach(ManagedConnection::shutdownGracefully);
            completeGracefulShutdownIfReady();
        }
        return shutdownCompletion;
    }

    @Override
    public java.util.OptionalLong reportActiveTasks() {
        return java.util.OptionalLong.of(activeTasks.get());
    }

    @PreDestroy
    @Override
    public void close() {
        managedConnections.values().forEach(ManagedConnection::close);
        managedConnections.clear();
        shutdownCompletion.complete(null);
    }

    private void completeGracefulShutdownIfReady() {
        if (!gracefulShutdown.get()) {
            return;
        }
        if (activeTasks.get() != 0L) {
            return;
        }
        boolean allStopped = managedConnections.values().stream().allMatch(ManagedConnection::gracefulShutdownComplete);
        if (allStopped) {
            shutdownCompletion.complete(null);
        }
    }

    /**
     * A subscription target.
     *
     * @param value   The channel or pattern
     * @param pattern Whether the subscription is pattern-based
     */
    public record ChannelSubscription(String value, boolean pattern) {
    }

    private final class ManagedConnection extends RedisPubSubAdapter<byte[], byte[]> implements AutoCloseable {
        private final StatefulRedisPubSubConnection<byte[], byte[]> connection;
        private final RedisPubSubCommands<byte[], byte[]> sync;
        private final Map<String, List<ListenerRegistration>> channels = new ConcurrentHashMap<>();
        private final Map<String, List<ListenerRegistration>> patterns = new ConcurrentHashMap<>();
        private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet();
        private final Set<String> subscribedPatterns = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean shutdownRequested = new AtomicBoolean();
        private final AtomicBoolean shutdownComplete = new AtomicBoolean();

        private ManagedConnection(Optional<String> connectionName) {
            this.connection = RedisConnectionUtil.openBytesRedisPubSubConnection(
                beanLocator,
                connectionName,
                "No Redis server configured for Pub/Sub listeners."
            );
            this.sync = connection.sync();
            this.connection.addListener(this);
        }

        private synchronized void subscribe(Set<ChannelSubscription> subscriptions,
                                            ExecutorService executor,
                                            Consumer<RedisMessage> consumer) {
            if (shutdownRequested.get()) {
                throw new IllegalStateException("Redis Pub/Sub connection is shutting down");
            }
            List<byte[]> newChannels = new ArrayList<>();
            List<byte[]> newPatterns = new ArrayList<>();
            for (ChannelSubscription subscription : subscriptions) {
                Map<String, List<ListenerRegistration>> registrations = subscription.pattern() ? patterns : channels;
                registrations.computeIfAbsent(subscription.value(), ignored -> new CopyOnWriteArrayList<>())
                    .add(new ListenerRegistration(executor, consumer));
                if (subscription.pattern()) {
                    if (subscribedPatterns.add(subscription.value())) {
                        newPatterns.add(subscription.value().getBytes(StandardCharsets.UTF_8));
                    }
                } else if (subscribedChannels.add(subscription.value())) {
                    newChannels.add(subscription.value().getBytes(StandardCharsets.UTF_8));
                }
            }
            if (!newChannels.isEmpty()) {
                sync.subscribe(newChannels.toArray(byte[][]::new));
            }
            if (!newPatterns.isEmpty()) {
                sync.psubscribe(newPatterns.toArray(byte[][]::new));
            }
        }

        @Override
        public void message(byte[] channel, byte[] message) {
            dispatch(null, new String(channel, StandardCharsets.UTF_8), message, channels);
        }

        @Override
        public void message(byte[] pattern, byte[] channel, byte[] message) {
            dispatch(
                new String(pattern, StandardCharsets.UTF_8),
                new String(channel, StandardCharsets.UTF_8),
                message,
                patterns
            );
        }

        private void dispatch(@Nullable String pattern,
                              String channel,
                              byte[] message,
                              Map<String, List<ListenerRegistration>> registrations) {
            String key = pattern == null ? channel : pattern;
            List<ListenerRegistration> listenerRegistrations = registrations.get(key);
            if (listenerRegistrations == null) {
                return;
            }
            RedisMessage redisMessage = new RedisMessage(message, channel, pattern);
            for (ListenerRegistration registration : listenerRegistrations) {
                activeTasks.incrementAndGet();
                try {
                    registration.executor().submit(() -> {
                        try {
                            registration.consumer().accept(redisMessage);
                        } catch (Exception e) {
                            LOG.error("Error dispatching Redis Pub/Sub message for channel [{}]", channel, e);
                        } finally {
                            if (activeTasks.decrementAndGet() == 0L) {
                                completeGracefulShutdownIfReady();
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    if (activeTasks.decrementAndGet() == 0L) {
                        completeGracefulShutdownIfReady();
                    }
                    LOG.warn("Executor rejected Redis Pub/Sub message for channel [{}] during shutdown", channel, e);
                }
            }
        }

        private synchronized void shutdownGracefully() {
            if (!shutdownRequested.compareAndSet(false, true)) {
                return;
            }
            try {
                if (!subscribedChannels.isEmpty()) {
                    sync.unsubscribe(subscribedChannels.stream().map(v -> v.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new));
                }
                if (!subscribedPatterns.isEmpty()) {
                    sync.punsubscribe(subscribedPatterns.stream().map(v -> v.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new));
                }
            } catch (Exception e) {
                LOG.warn("Error while unsubscribing Redis Pub/Sub listeners during graceful shutdown", e);
            } finally {
                shutdownComplete.set(true);
                completeGracefulShutdownIfReady();
            }
        }

        private boolean gracefulShutdownComplete() {
            return shutdownComplete.get();
        }

        @Override
        public void close() {
            shutdownComplete.set(true);
            connection.close();
        }
    }

    private record ListenerRegistration(ExecutorService executor, Consumer<RedisMessage> consumer) {
    }
}
