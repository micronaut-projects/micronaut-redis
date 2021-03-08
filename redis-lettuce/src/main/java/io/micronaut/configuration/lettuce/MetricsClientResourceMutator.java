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

import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Requires;

import javax.inject.Singleton;

/**
 * Mutates a {@link ClientResources.Builder} adding {@link MicrometerCommandLatencyRecorder}.
 * @author Rafael Acevedo
 * @since 4.1
 */
@Singleton
@Requires(beans = MeterRegistry.class)
public class MetricsClientResourceMutator implements ClientResourcesMutator {
    private final MeterRegistry meterRegistry;

    public MetricsClientResourceMutator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void mutate(ClientResources.Builder builder, AbstractRedisConfiguration config) {
        MicrometerOptions options = MicrometerOptions.builder().histogram(true).build();
        builder.commandLatencyRecorder(new MicrometerCommandLatencyRecorder(meterRegistry, options));
    }
}
