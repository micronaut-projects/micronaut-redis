package io.micronaut.configuration.lettuce;

import io.micronaut.cache.SyncCache;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.ApplicationConfiguration;

@ConfigurationProperties(RedisSetting.REDIS_POOL)
@Requires(classes = SyncCache.class)
public class DefaultRedisConnectionPoolConfiguration extends AbstractRedisConnectionPoolConfiguration {
    /**
     * Constructor.
     *
     * @param applicationConfiguration applicationConfiguration
     */
    public DefaultRedisConnectionPoolConfiguration(ApplicationConfiguration applicationConfiguration) {
    }
}
