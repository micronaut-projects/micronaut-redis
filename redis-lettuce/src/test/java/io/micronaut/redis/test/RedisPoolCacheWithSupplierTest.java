package io.micronaut.redis.test;

import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.CacheManager;
import io.micronaut.cache.SyncCache;
import io.micronaut.configuration.lettuce.cache.RedisCache;
import io.micronaut.configuration.lettuce.cache.RedisCacheConfiguration;
import io.micronaut.configuration.lettuce.cache.RedisConnectionPoolCache;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.util.StringUtils;
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
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest(startApplication = false)
@Property(name = "redis.pool.enabled", value = StringUtils.TRUE)
@Property(name = "redis.caches.default.expire-after-write", value = "1s")
@Property(name = "redis.caches.default.expire-after-access", value = "2s")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
class RedisPoolCacheWithSupplierTest implements TestPropertyProvider {
    @Container
    static GenericContainer redis = new GenericContainer<>(DockerImageName.parse("redis:latest"))
        .withExposedPorts(6379);

    @Inject
    BeanContext beanContext;

    @Inject
    CacheManager<?> cacheManager;

    @Test
    void expiredItemIsNotRetrievedFromCache() throws ExecutionException, InterruptedException {
        assertTrue(beanContext.containsBean(RedisConnectionPoolCache.class));
        assertFalse(beanContext.containsBean(RedisCache.class));
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
        Supplier<String> supplier = () -> "default-value";
        cache.put("key", "cached-value");
        String cachedValue1 = cache.get("key", String.class, supplier);

        assertEquals("cached-value", cachedValue1);

        // sleep for more time than expire-after-write and expire-after-access
        Thread.sleep(3000);

        cachedValue1 = cache.get("key", String.class, supplier);

        assertEquals("default-value", cachedValue1);

        AsyncCache asyncCache = cache.async();

        asyncCache.put("key", "cached-value");
        CompletableFuture<String> completableFuture = asyncCache.get("key", String.class, supplier);

        String cachedValue = completableFuture.get();
        assertEquals("cached-value", cachedValue);

        // sleep for more time than expire-after-write and expire-after-access
        Thread.sleep(3000);

        completableFuture = asyncCache.get("key", String.class, supplier);

        // Verify that the value is no longer present
        cachedValue = completableFuture.get();
        assertEquals("default-value", cachedValue);

    }

    @Override
    public @NonNull Map<String, String> getProperties() {
        redis.start();
        String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
        return Map.of("redis.uri", redisUrl, "redis.port",  "6379");
    }
}
