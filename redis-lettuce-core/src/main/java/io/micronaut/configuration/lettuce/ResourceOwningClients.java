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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.env.DevelopmentActive;
import io.micronaut.context.reload.BeanRetentionPolicy;
import io.micronaut.core.annotation.Internal;
import jakarta.inject.Singleton;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

/**
 * The Redis clients the factories create in development mode on the {@link ClientResources} they build for them. A bean
 * of development mode only, whose presence tells the factories that it is on: it holds no state, so neither a factory
 * that receives it nor the clients development mode keeps across a restart are bound to the context. Outside
 * development mode the bean is absent, and the factories build the resources on the calling thread and create plain
 * Lettuce clients, which leave the resources running, as they always did. Lettuce leaves the
 * resources it was given running when a client shuts down, as they may be shared; the factories build these for
 * the one client, so the client shuts them down with itself: their event executors and timer would otherwise keep
 * running after the client is gone. What the built resources share with resources they were mutated from, such as
 * the event loops of a {@link ClientResources} bean, is left running, as Lettuce does for shared components.
 *
 * <p>The clients also copy the {@link RedisURI} they connect to, rather than keep the configuration bean, which is
 * a {@link RedisURI} too: a client that development mode keeps across a restart holds no configuration of the
 * stopped application.</p>
 *
 * <p>A URI can carry a {@link RedisCredentialsProvider} that the application set on the configuration, which the
 * context does not see, since it is not injected. As the {@link BeanRetentionPolicy} of these clients, this bean
 * refuses to keep a client whose URIs carry a credentials provider of a class of the application: the client would
 * keep asking the stopped application for credentials, and keep it reachable.</p>
 *
 * @author graemerocher
 * @since 7.3.0
 */
@Internal
@Singleton
@DevelopmentActive
final class ResourceOwningClients implements BeanRetentionPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceOwningClients.class);

    @Override
    public Decision decide(BeanRegistration<?> registration) {
        Object bean = registration.bean();
        String credentials = null;
        if (bean instanceof OwningRedisClient client) {
            credentials = client.applicationCredentials;
        } else if (bean instanceof OwningRedisClusterClient client) {
            credentials = client.applicationCredentials;
        }
        if (credentials == null) {
            return Decision.ABSTAIN;
        }
        LOG.debug("The Redis client [{}] is not retained across the restart: its credentials provider {} is a class of the application", bean, credentials);
        return Decision.REFUSE;
    }

    /**
     * Creates a standalone client that owns the resources.
     *
     * @param resources The resources built for the client
     * @param uri The URI, copied
     * @return The client
     */
    RedisClient redisClient(ClientResources resources, RedisURI uri) {
        return new OwningRedisClient(resources, copy(uri), applicationCredentials(List.of(uri)));
    }

    /**
     * Creates a cluster client that owns the resources.
     *
     * @param resources The resources built for the client
     * @param uris The URIs, copied
     * @return The client
     */
    RedisClusterClient redisClusterClient(ClientResources resources, Iterable<RedisURI> uris) {
        List<RedisURI> copies = new ArrayList<>();
        for (RedisURI uri : uris) {
            copies.add(copy(uri));
        }
        return new OwningRedisClusterClient(resources, copies, applicationCredentials(copies));
    }

    /**
     * The class of the first credentials provider of the URIs, and of their sentinels, that is a class of the
     * application: one loaded by neither the class loader of Lettuce nor one of its parents.
     */
    private static @Nullable String applicationCredentials(Iterable<RedisURI> uris) {
        for (RedisURI uri : uris) {
            RedisCredentialsProvider provider = uri.getCredentialsProvider();
            if (provider != null && isApplicationClass(provider.getClass())) {
                return provider.getClass().getName();
            }
            String sentinel = applicationCredentials(uri.getSentinels());
            if (sentinel != null) {
                return sentinel;
            }
        }
        return null;
    }

    private static boolean isApplicationClass(Class<?> type) {
        ClassLoader loader = type.getClassLoader();
        if (loader == null) {
            return false;
        }
        for (ClassLoader library = RedisClient.class.getClassLoader(); library != null; library = library.getParent()) {
            if (library == loader) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds the resources of a client. When the calling thread's context class loader is not the one Lettuce is loaded
     * by, such as an application class loader of development mode, which reloads the application's classes in a loader
     * of their own, they are built on a thread of their own, whose stack and context class loader are the library's:
     * Netty records where its timer was created, in a trace that keeps the classes on the stack reachable, and the
     * timer's thread takes the context class loader of the thread that created it. The resources of a client
     * development mode keeps across a restart would otherwise keep the application that created them reachable.
     * Otherwise, as on a flat class path, they are built on the calling thread.
     *
     * @param builder The builder
     * @return The resources
     */
    ClientResources build(ClientResources.Builder builder) {
        ClassLoader library = ResourceOwningClients.class.getClassLoader();
        if (Thread.currentThread().getContextClassLoader() == library) {
            return builder.build();
        }
        AtomicReference<ClientResources> built = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                built.set(builder.build());
            } catch (Throwable e) {
                failure.set(e);
            }
        }, "redis-client-resources");
        thread.setContextClassLoader(library);
        thread.setDaemon(true);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while building the Redis client resources", e);
        }
        Throwable e = failure.get();
        if (e instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (e instanceof Error error) {
            throw error;
        }
        return built.get();
    }

    /**
     * Copies what a client reads from a URI. {@link RedisURI#builder(RedisURI)} leaves out the sentinels, the
     * sentinel master and the TLS settings, so the properties are copied one by one.
     */
    private static RedisURI copy(RedisURI uri) {
        RedisURI copy = new RedisURI();
        if (uri.getHost() != null) {
            copy.setHost(uri.getHost());
        }
        copy.setPort(uri.getPort());
        if (uri.getSocket() != null) {
            copy.setSocket(uri.getSocket());
        }
        if (uri.getSentinelMasterId() != null) {
            copy.setSentinelMasterId(uri.getSentinelMasterId());
        }
        for (RedisURI sentinel : uri.getSentinels()) {
            copy.getSentinels().add(copy(sentinel));
        }
        copy.setDatabase(uri.getDatabase());
        if (uri.getClientName() != null) {
            copy.setClientName(uri.getClientName());
        }
        if (uri.getDriverInfo() != null) {
            copy.setDriverInfo(uri.getDriverInfo());
        }
        // kept apart from the driver information
        if (uri.getLibraryVersion() != null) {
            copy.setLibraryVersion(uri.getLibraryVersion());
        }
        if (uri.getTimeout() != null) {
            copy.setTimeout(uri.getTimeout());
        }
        copy.applySsl(uri);
        if (uri.getCredentialsProvider() != null) {
            copy.setCredentialsProvider(uri.getCredentialsProvider());
        }
        return copy;
    }

    private static CompletableFuture<Void> thenShutdownResources(CompletableFuture<Void> client, ClientResources resources,
                                                    long quietPeriod, long timeout, TimeUnit timeUnit) {
        return client.handle((ignored, failure) -> failure)
            .thenCompose(failure -> {
                CompletableFuture<Void> resourcesShutdown = new CompletableFuture<>();
                resources.shutdown(quietPeriod, timeout, timeUnit).addListener(future -> {
                    if (failure != null) {
                        resourcesShutdown.completeExceptionally(failure);
                    } else if (future.isSuccess()) {
                        resourcesShutdown.complete(null);
                    } else {
                        resourcesShutdown.completeExceptionally(future.cause());
                    }
                });
                return resourcesShutdown;
            });
    }

    /**
     * A standalone client that shuts down its resources with itself.
     */
    private static final class OwningRedisClient extends RedisClient {
        private final ClientResources resources;
        private final @Nullable String applicationCredentials;

        OwningRedisClient(ClientResources resources, RedisURI uri, @Nullable String applicationCredentials) {
            super(resources, uri);
            this.resources = resources;
            this.applicationCredentials = applicationCredentials;
        }

        @Override
        public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {
            return thenShutdownResources(super.shutdownAsync(quietPeriod, timeout, timeUnit), resources, quietPeriod, timeout, timeUnit);
        }
    }

    /**
     * A cluster client that shuts down its resources with itself.
     */
    private static final class OwningRedisClusterClient extends RedisClusterClient {
        private final ClientResources resources;
        private final @Nullable String applicationCredentials;

        OwningRedisClusterClient(ClientResources resources, Iterable<RedisURI> uris, @Nullable String applicationCredentials) {
            super(resources, uris);
            this.resources = resources;
            this.applicationCredentials = applicationCredentials;
        }

        @Override
        public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {
            return thenShutdownResources(super.shutdownAsync(quietPeriod, timeout, timeUnit), resources, quietPeriod, timeout, timeUnit);
        }
    }
}
