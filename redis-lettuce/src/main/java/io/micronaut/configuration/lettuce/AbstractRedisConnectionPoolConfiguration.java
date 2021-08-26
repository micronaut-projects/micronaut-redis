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
package io.micronaut.configuration.lettuce;

import java.util.Optional;

/**
 * Allows configuration of caches stored in Redis.
 *
 * @author Graeme Rocher, Alex Katlein
 * @since 1.3
 */
public abstract class AbstractRedisConnectionPoolConfiguration {

    protected Integer minIdle;
    protected Integer maxIdle;
    protected Integer maxTotal;
    protected Boolean enabled;

    /**
     * @return The minimum idle connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public Optional<Integer> getMinIdle() {
        if (minIdle != null) {
            return Optional.of(minIdle);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The maximum idle connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public Optional<Integer> getMaxIdle() {
        if (maxIdle != null) {
            return Optional.of(maxIdle);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The maximum total connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public Optional<Integer> getMaxTotal() {
        if (maxTotal != null) {
            return Optional.of(maxTotal);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The maximum total connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public Optional<Boolean> getEnabled() {
        if (enabled != null) {
            return Optional.of(enabled);
        } else {
            return Optional.empty();
        }
    }
}
