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

import javax.inject.Singleton;

/**
 * Mutates a {@link ClientResources.Builder} adding lettuce threadpool configs.
 * @author Rafael Acevedo
 */
@Singleton
public class ThreadPoolClientResourceMutator implements ClientResourcesMutator {

    @Override
    public void mutate(ClientResources.Builder builder, AbstractRedisConfiguration config) {
        if (config.getIoThreadPoolSize() != null) {
            builder.ioThreadPoolSize(config.getIoThreadPoolSize());
        }
        if (config.getComputationThreadPoolSize() != null) {
            builder.computationThreadPoolSize(config.getComputationThreadPoolSize());
        }
    }
}
