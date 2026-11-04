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
package io.micronaut.redis.dev;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micronaut.configuration.lettuce.RedisModuleConnections;
import io.micronaut.configuration.lettuce.cache.RedisCache;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.reload.ClassChange;
import io.micronaut.context.reload.ClassChangeEvent;
import io.micronaut.context.reload.ReloadStrategy;
import io.micronaut.dev.tck.ReloadHarness;
import io.micronaut.dev.tck.ReloadTck;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.redis.testcontainers.Redis;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs an application with a Redis Pub/Sub listener, a listener of its own on the Pub/Sub connection bean of a named
 * server, and a Redis cache through the development runtime, against a Redis server, and restarts it. Development mode
 * keeps the clients, their resources and their connections across the restart; the listeners of the stopped
 * generation are removed from the connections it kept, so that the next one receives each message once, and nothing of
 * the stopped generation stays reachable. A change under {@code redis} releases what was kept.
 */
class RedisReloadTest {

    private static final String CHANNEL = "dev-reload";
    private static final String MANUAL_CHANNEL = "dev-reload-manual";

    private static final String LISTENER = """
        package example;

        import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel;
        import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener;
        import io.micronaut.http.MediaType;
        import io.micronaut.http.annotation.Consumes;
        import io.micronaut.messaging.annotation.MessageBody;

        import java.util.List;
        import java.util.concurrent.CopyOnWriteArrayList;

        @RedisListener
        public class Listener {
            private final List<String> received = new CopyOnWriteArrayList<>();

            @Consumes(MediaType.TEXT_PLAIN)
            @MessageChannel("%s")
            void receive(@MessageBody String value) {
                received.add("%s " + value);
            }

            public List<String> received() {
                return received;
            }
        }
        """;

    private static final String MANUAL_LISTENER = """
        package example;

        import io.lettuce.core.pubsub.RedisPubSubAdapter;
        import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
        import io.micronaut.context.annotation.Context;
        import jakarta.inject.Named;

        import java.util.List;
        import java.util.concurrent.CopyOnWriteArrayList;

        @Context
        public class ManualListener extends RedisPubSubAdapter<String, String> {
            private final List<String> received = new CopyOnWriteArrayList<>();

            ManualListener(@Named("other") StatefulRedisPubSubConnection<String, String> connection) {
                connection.addListener(this);
                connection.sync().subscribe("%s");
            }

            @Override
            public void message(String channel, String message) {
                received.add("%s " + message);
            }

            public List<String> received() {
                return received;
            }
        }
        """;

    private static final String CODEC = """
        package example;

        import io.lettuce.core.codec.RedisCodec;
        import io.lettuce.core.codec.StringCodec;
        import jakarta.inject.Named;
        import jakarta.inject.Singleton;

        import java.nio.ByteBuffer;

        @Singleton
        @Named("other")
        public class OtherCodec implements RedisCodec<String, String> {
            private static final String GENERATION = "%s";

            @Override
            public String decodeKey(ByteBuffer bytes) {
                return StringCodec.UTF8.decodeKey(bytes);
            }

            @Override
            public String decodeValue(ByteBuffer bytes) {
                return StringCodec.UTF8.decodeValue(bytes);
            }

            @Override
            public ByteBuffer encodeKey(String key) {
                return StringCodec.UTF8.encodeKey(key);
            }

            @Override
            public ByteBuffer encodeValue(String value) {
                return StringCodec.UTF8.encodeValue(value);
            }
        }
        """;

    private static RedisClient publisherClient;
    private static StatefulRedisConnection<String, String> publisher;

    @TempDir
    Path project;

    @BeforeAll
    static void connect() {
        publisherClient = RedisClient.create(Redis.getProperties().get("redis.uri"));
        publisher = publisherClient.connect();
    }

    @AfterAll
    static void disconnect() {
        publisher.close();
        publisherClient.shutdown();
    }

    @Test
    void aRestartKeepsTheClientsAndConnectionsAndTheNextGenerationReceivesEachMessageOnce() throws Exception {
        try (ReloadHarness harness = ReloadHarness.inDirectory(project)) {
            properties(harness);
            harness.source("example.Listener", LISTENER.formatted(CHANNEL, "first"));
            harness.source("example.ManualListener", MANUAL_LISTENER.formatted(MANUAL_CHANNEL, "first"));
            harness.start();
            assertReloadersPresent(harness.context());

            awaitTrue("the first generation subscribes", () -> subscribers(CHANNEL) == 1 && subscribers(MANUAL_CHANNEL) == 1);
            publisher.sync().publish(CHANNEL, "one");
            publisher.sync().publish(MANUAL_CHANNEL, "one");
            awaitTrue("the first generation receives", () -> received(harness.context(), "example.Listener").contains("first one")
                && received(harness.context(), "example.ManualListener").contains("first one"));

            ApplicationContext first = harness.context();
            RedisClient client = first.getBean(RedisClient.class);
            ClientResources resources = client.getResources();
            StatefulRedisConnection<?, ?> connection = first.getBean(StatefulRedisConnection.class);
            RedisClient namedClient = first.getBean(RedisClient.class, Qualifiers.byName("other"));
            StatefulRedisConnection<?, ?> namedConnection = first.getBean(StatefulRedisConnection.class, Qualifiers.byName("other"));
            StatefulRedisPubSubConnection<?, ?> namedPubSubConnection = first.getBean(StatefulRedisPubSubConnection.class, Qualifiers.byName("other"));
            RedisModuleConnections moduleConnections = first.getBean(RedisModuleConnections.class);
            RedisCache cache = first.getBean(RedisCache.class, Qualifiers.byName("books"));
            Object cacheConnection = cache.getNativeCache();
            cache.put("key", "value");
            List<String> firstReceived = received(first, "example.Listener");
            List<String> firstManualReceived = received(first, "example.ManualListener");
            first = null;

            harness.source("example.Listener", LISTENER.formatted(CHANNEL, "second"));
            harness.source("example.ManualListener", MANUAL_LISTENER.formatted(MANUAL_CHANNEL, "second"));
            harness.reload();
            assertEquals(2, harness.generation());
            ApplicationContext second = harness.context();
            assertReloadersPresent(second);

            // the clients, their resources and their connections are those of the first generation
            for (Object retained : List.of(client, connection, namedClient, namedConnection, namedPubSubConnection, moduleConnections)) {
                ReloadTck.assertRetained(harness, retained);
            }
            assertSame(client, second.getBean(RedisClient.class));
            assertSame(resources, second.getBean(RedisClient.class).getResources());
            assertSame(connection, second.getBean(StatefulRedisConnection.class));
            assertSame(namedClient, second.getBean(RedisClient.class, Qualifiers.byName("other")));
            assertSame(namedConnection, second.getBean(StatefulRedisConnection.class, Qualifiers.byName("other")));
            assertSame(namedPubSubConnection, second.getBean(StatefulRedisPubSubConnection.class, Qualifiers.byName("other")));
            assertTrue(connection.isOpen());

            // the cache is created again, from the serializers of the new generation, on the same connection
            RedisCache secondCache = second.getBean(RedisCache.class, Qualifiers.byName("books"));
            assertNotSame(cache, secondCache);
            assertSame(cacheConnection, secondCache.getNativeCache());
            assertEquals("value", secondCache.get("key", String.class).orElse(null));
            cache = null;
            secondCache = null;

            // the subscriptions of the first generation went with its listeners: each channel has one subscriber
            awaitTrue("the second generation subscribes", () -> subscribers(CHANNEL) == 1 && subscribers(MANUAL_CHANNEL) == 1);
            publisher.sync().publish(CHANNEL, "two");
            publisher.sync().publish(MANUAL_CHANNEL, "two");
            awaitTrue("the second generation receives", () -> received(harness.context(), "example.Listener").contains("second two")
                && received(harness.context(), "example.ManualListener").contains("second two"));
            // a message received twice would have arrived with the first copy
            Thread.sleep(500);
            assertEquals(List.of("second two"), received(harness.context(), "example.Listener"));
            assertEquals(List.of("second two"), received(harness.context(), "example.ManualListener"));
            assertEquals(List.of("first one"), firstReceived, "the retired listener received nothing more");
            assertEquals(List.of("first one"), firstManualReceived, "the retired listener of the connection received nothing more");
            second = null;

            // neither the listeners of the first generation, removed from the connections, nor what development mode
            // kept, keep it reachable
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
    }

    @Test
    void aChangeUnderTheRedisPrefixReleasesTheClientsAndTheirResources() throws Exception {
        try (ReloadHarness harness = ReloadHarness.inDirectory(project)) {
            properties(harness);
            harness.source("example.Listener", LISTENER.formatted(CHANNEL, "first"));
            harness.start();
            awaitTrue("the first generation subscribes", () -> subscribers(CHANNEL) == 1);
            RedisClient first = harness.context().getBean(RedisClient.class);
            StatefulRedisConnection<?, ?> firstConnection = harness.context().getBean(StatefulRedisConnection.class);
            ClientResources[] firstResources = {first.getResources()};

            // the configuration changes, together with a class, so the application restarts
            StringBuilder changed = new StringBuilder();
            configuration().forEach((key, value) -> changed.append(key).append('=').append(value).append('\n'));
            changed.append("redis.timeout=17s\n");
            harness.resource("application.properties", changed.toString());
            harness.source("example.Listener", LISTENER.formatted(CHANNEL, "second"));
            harness.reload();
            assertEquals(2, harness.generation());

            RedisClient second = harness.context().getBean(RedisClient.class);
            assertNotSame(first, second);
            assertNotSame(firstConnection, harness.context().getBean(StatefulRedisConnection.class));
            assertFalse(firstConnection.isOpen(), "the released connection is closed");
            // the client shut down the resources it was built on with itself
            awaitTrue("the released resources shut down", () -> firstResources[0].eventExecutorGroup().isTerminated());
            assertEquals(Duration.ofSeconds(17), harness.context().getBean(StatefulRedisConnection.class).getTimeout());
            first = null;
            firstConnection = null;
            firstResources[0] = null;

            awaitTrue("the second generation subscribes", () -> subscribers(CHANNEL) == 1);
            publisher.sync().publish(CHANNEL, "two");
            awaitTrue("the second generation receives on its connection", () -> received(harness.context(), "example.Listener").equals(List.of("second two")));
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
    }

    @Test
    void aListenerChangedInPlaceIsRegisteredAgainOnTheSameConnection() throws Exception {
        try (ReloadHarness harness = ReloadHarness.inDirectory(project)) {
            properties(harness);
            harness.source("example.Listener", LISTENER.formatted(CHANNEL, "first"));
            harness.start();
            awaitTrue("the listener subscribes", () -> subscribers(CHANNEL) == 1);
            Object listener = listener(harness.context());
            RedisCache cache = harness.context().getBean(RedisCache.class, Qualifiers.byName("books"));

            changedInPlace(harness, "example.Listener");

            assertEquals(1, harness.generation(), "the application did not restart");
            assertNotSame(listener, listener(harness.context()), "the listener bean is recreated");
            assertSame(cache, harness.context().getBean(RedisCache.class, Qualifiers.byName("books")), "a listener change leaves the caches");
            awaitTrue("the listener subscribes again", () -> subscribers(CHANNEL) == 1);
            publisher.sync().publish(CHANNEL, "two");
            awaitTrue("the recreated listener receives", () -> received(harness.context(), "example.Listener").contains("first two"));
            // a registration left by the previous processor would deliver it a second time
            Thread.sleep(500);
            assertEquals(List.of("first two"), received(harness.context(), "example.Listener"));
            assertEquals(List.of(), received(listener), "the replaced listener received nothing more");
        }
    }

    private static Map<String, String> configuration() {
        String uri = Redis.getProperties().get("redis.uri");
        return Map.of(
            "redis.uri", uri,
            "redis.servers.other.uri", uri,
            "redis.caches.books.expire-after-write", "1h"
        );
    }

    private static void properties(ReloadHarness harness) {
        configuration().forEach(harness::property);
    }

    private static long subscribers(String channel) {
        Map<String, Long> counts = publisher.sync().pubsubNumsub(channel);
        Long count = counts.get(channel);
        return count == null ? 0 : count;
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting until " + what);
            }
            Thread.sleep(100);
        }
    }

    /**
     * Tells the running generation that a class was redefined in place, as the development runtime does after it
     * redefined the class. The context is not kept: a reference to it would keep the generation reachable.
     */
    private static void changedInPlace(ReloadHarness harness, String className) {
        ApplicationContext context = harness.context();
        context.publishEvent(new ClassChangeEvent(RedisReloadTest.class, Set.of(), context.getClassLoader(),
            List.of(new ClassChange(className, ClassChange.Kind.MODIFIED)), ReloadStrategy.RELOAD));
    }

    private static Object listener(ApplicationContext context) {
        return context.getBean(type(context, "example.Listener"));
    }

    private static void assertReloadersPresent(ApplicationContext context) {
        // the bean that follows the changes exists in development mode only
        String reloader = "io.micronaut.configuration.lettuce.DevelopmentRedisReloader";
        assertTrue(context.containsBean(type(context, reloader)), reloader);
    }

    private static List<String> received(ApplicationContext context, String className) {
        return received(context.getBean(type(context, className)));
    }

    @SuppressWarnings("unchecked")
    private static List<String> received(Object bean) {
        try {
            return (List<String>) bean.getClass().getMethod("received").invoke(bean);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Cannot read what " + bean + " received", e);
        }
    }

    private static Class<?> type(ApplicationContext context, String className) {
        try {
            return Class.forName(className, true, context.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new AssertionError(className + " is not in the application", e);
        }
    }
}
