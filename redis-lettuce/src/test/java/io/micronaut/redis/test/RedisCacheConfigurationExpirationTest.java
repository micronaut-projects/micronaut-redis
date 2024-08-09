package io.micronaut.redis.test;

import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.CacheManager;
import io.micronaut.cache.SyncCache;
import io.micronaut.configuration.lettuce.cache.DefaultRedisCacheConfiguration;
import io.micronaut.configuration.lettuce.cache.RedisCache;
import io.micronaut.configuration.lettuce.cache.RedisCacheConfiguration;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.util.StringUtils;
import io.micronaut.runtime.ApplicationConfiguration;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest(startApplication = false)
@Property(name = "redis.caches.default.expire-after-write", value = "1s")
@Property(name = "redis.caches.default.expire-after-access", value = "2s")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
class RedisCacheConfigurationExpirationTest implements TestPropertyProvider {
    @Container
    static GenericContainer redis = new GenericContainer<>(DockerImageName.parse("redis:latest"))
            .withExposedPorts(6379);

    @Inject
    BeanContext beanContext;

    @Inject
    CacheManager<?> cacheManager;

    @Test
    void expiredItemIsNotRetrievedFromCache() throws ExecutionException, InterruptedException {
        assertTrue(beanContext.containsBean(RedisCache.class));
        Collection<RedisCacheConfiguration> redisCacheConfigurations = beanContext.getBeansOfType(RedisCacheConfiguration.class);
        assertNotNull(redisCacheConfigurations);
        assertFalse(redisCacheConfigurations.isEmpty());
        assertEquals(1, redisCacheConfigurations.size());
        RedisCacheConfiguration redisCacheConfiguration = (RedisCacheConfiguration) redisCacheConfigurations.toArray()[0];
        assertTrue(redisCacheConfiguration.getExpireAfterAccess().isPresent());
        assertTrue(redisCacheConfiguration.getExpireAfterWrite().isPresent());
        assertEquals(Duration.ofSeconds(2), redisCacheConfiguration.getExpireAfterAccess().get());
        assertEquals(Duration.ofSeconds(1), redisCacheConfiguration.getExpireAfterWrite().get());

        SyncCache<?> cache = cacheManager.getCache("default");

        cache.put("Value1", "key");
        Optional<String> cachedValue = cache.get("Value1", String.class);

        assertEquals("key", cachedValue.orElse(null));

        // sleep for more time than expire-after-write and expire-after-access
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Optional<String> expiredValue = cache.get("Value1", String.class);

        assertEquals(Optional.empty(), expiredValue);

        AsyncCache asyncCache = cache.async();

        asyncCache.put("Value1", "key");
        CompletableFuture<Optional<String>> completableFuture = asyncCache.get("Value1", String.class);

        assertEquals("key", completableFuture.get().get());

        // sleep for more time than expire-after-write and expire-after-access
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        completableFuture = asyncCache.get("Value1", String.class);

        assertTrue(completableFuture.get().isEmpty());

    }

    @Override
    public @NonNull Map<String, String> getProperties() {
        redis.start();
        String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
        return Map.of("redis.uri", redisUrl, "redis.port",  "6379");
    }
}
