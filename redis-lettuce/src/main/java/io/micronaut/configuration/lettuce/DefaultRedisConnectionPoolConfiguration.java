/*
 * Copyright 2017-2021 original authors
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

import io.lettuce.core.support.BoundedPoolConfig;
import io.micronaut.cache.SyncCache;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.runtime.ApplicationConfiguration;

/**
 * Allows configuration of redis connection pool.
 *
 * @author Graeme Rocher, Illia Kovalov
 * @since 1.3
 */
@ConfigurationProperties(RedisSetting.REDIS_POOL)
@Requires(classes = SyncCache.class, property = RedisSetting.REDIS_POOL + ".enabled", defaultValue = StringUtils.FALSE, notEquals = StringUtils.FALSE)
@Primary
public class DefaultRedisConnectionPoolConfiguration extends AbstractRedisConnectionPoolConfiguration {
    /**
     * Constructor.
     *
     * @param applicationConfiguration applicationConfiguration
     */
    public DefaultRedisConnectionPoolConfiguration(ApplicationConfiguration applicationConfiguration) {
    }

    /**
     *
     * @return BoundedPoolConfig
     */
    public BoundedPoolConfig getBoundedPoolConfig() {
        BoundedPoolConfig.Builder builder = BoundedPoolConfig.builder();
        this.getMaxIdle().ifPresent(builder::maxIdle);
        this.getMinIdle().ifPresent(builder::minIdle);
        this.getMaxTotal().ifPresent(builder::maxTotal);
        return builder.build();
    }
}
