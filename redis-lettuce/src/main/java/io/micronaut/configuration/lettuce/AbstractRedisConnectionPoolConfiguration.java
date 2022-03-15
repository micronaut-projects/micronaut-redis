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

import io.micronaut.core.annotation.NonNull;

import java.util.Optional;

/**
 * Allows configuration of caches stored in Redis.
 *
 * @author Graeme Rocher, Alex Katlein, Illia Kovalov
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
    public @NonNull Optional<Integer> getMinIdle() {
        return Optional.ofNullable(minIdle);
    }

    /**
     * @return The maximum idle connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public @NonNull Optional<Integer> getMaxIdle() {
        return Optional.ofNullable(maxIdle);
    }

    /**
     * @return The maximum total connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public @NonNull Optional<Integer> getMaxTotal() {
        return Optional.ofNullable(maxTotal);
    }

    /**
     * @return The maximum total connections count.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     */
    public @NonNull Optional<Boolean> getEnabled() {
        return Optional.ofNullable(enabled);
    }
}
