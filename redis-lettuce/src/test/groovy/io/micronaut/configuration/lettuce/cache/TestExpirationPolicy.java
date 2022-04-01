package io.micronaut.configuration.lettuce.cache;

import io.micronaut.configuration.lettuce.cache.expiration.ExpirationAfterWritePolicy;

import jakarta.inject.Singleton;

@Singleton
public class TestExpirationPolicy implements ExpirationAfterWritePolicy {

    @Override
    public long getExpirationAfterWrite(Object value) {
        return 1500;
    }
}
