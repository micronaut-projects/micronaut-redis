/*
 * Copyright 2017-2020 original authors
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
package io.micronaut.configuration.lettuce.cache;

import io.micronaut.cache.SyncCache;
import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.ApplicationConfiguration;

/**
 * Allows providing default configuration values for the actual caches.
 *
 * @author Alex Katlein
 * @since 1.3
 */
@ConfigurationProperties(RedisSetting.REDIS_CACHE)
@Requires(classes = SyncCache.class)
public class DefaultRedisCacheConfiguration extends AbstractRedisCacheConfiguration {
    /**
     * Constructor.
     *
     * @param applicationConfiguration applicationConfiguration
     */
    public DefaultRedisCacheConfiguration(ApplicationConfiguration applicationConfiguration) {
        super(applicationConfiguration);
    }
}
