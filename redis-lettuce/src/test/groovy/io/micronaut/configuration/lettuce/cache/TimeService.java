package io.micronaut.configuration.lettuce.cache;

import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.Cacheable;

import jakarta.inject.Singleton;

@Singleton
@CacheConfig(cacheNames = {"time"})
public class TimeService {
    @Cacheable("test1")
    public long getTimeWithConstantExpirationPolicy() {
        return System.nanoTime();
    }

    @Cacheable("test2")
    public long getTimeWithDynamicExpirationPolicy() {
        return System.nanoTime();
    }
}
