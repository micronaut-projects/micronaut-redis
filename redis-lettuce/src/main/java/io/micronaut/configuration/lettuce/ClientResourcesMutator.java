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

import io.lettuce.core.resource.ClientResources;
import io.micronaut.core.annotation.Indexed;

/**
 * Mutates a {@link ClientResources.Builder}.
 * @author Rafael Acevedo
 * @since 4.1
 */
@Indexed(ClientResourcesMutator.class)
public interface ClientResourcesMutator {

    /**
     * Mutates a {@link ClientResources.Builder}.
     * @param builder the builder
     * @param config the redis config
     */
    void mutate(ClientResources.Builder builder, AbstractRedisConfiguration config);
}
