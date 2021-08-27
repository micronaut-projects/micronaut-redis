package io.micronaut.configuration.lettuce;

import io.micronaut.cache.SyncCache;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.ApplicationConfiguration;

@ConfigurationProperties(RedisSetting.REDIS_POOL)
@Requires(classes = SyncCache.class)

/**
 * Allows configuration of redis connection pool.
 *
 * @author Graeme Rocher, Illia Kovalov
 * @since 1.3
 */
public class DefaultRedisConnectionPoolConfiguration extends AbstractRedisConnectionPoolConfiguration {
    /**
     * Constructor.
     *
     * @param applicationConfiguration applicationConfiguration
     */
    public DefaultRedisConnectionPoolConfiguration(ApplicationConfiguration applicationConfiguration) {
    }
}
