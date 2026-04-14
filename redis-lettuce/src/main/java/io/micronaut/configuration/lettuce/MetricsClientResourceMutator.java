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
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.env.Environment;
import io.micronaut.inject.qualifiers.Qualifiers;

import jakarta.inject.Singleton;

/**
 * Mutates a {@link ClientResources.Builder} adding {@link MicrometerCommandLatencyRecorder}.
 * @author Rafael Acevedo
 * @since 4.1
 */
@Singleton
@Requires(beans = MeterRegistry.class)
public class MetricsClientResourceMutator implements ClientResourcesMutator {
    private final MeterRegistry meterRegistry;
    private final BeanLocator beanLocator;
    private final AbstractRedisConfiguration.RedisCommandLatencyRecorderConfiguration defaultConfiguration;

    public MetricsClientResourceMutator(MeterRegistry meterRegistry,
                                        BeanLocator beanLocator,
                                        @Primary AbstractRedisConfiguration.RedisCommandLatencyRecorderConfiguration defaultConfiguration) {
        this.meterRegistry = meterRegistry;
        this.beanLocator = beanLocator;
        this.defaultConfiguration = defaultConfiguration;
    }

    @Override
    public void mutate(ClientResources.Builder builder, AbstractRedisConfiguration config) {
        AbstractRedisConfiguration.RedisCommandLatencyRecorderConfiguration recorderConfiguration =
                config.getName().equals(Environment.DEFAULT_NAME)
                        ? defaultConfiguration
                        : beanLocator.findBean(AbstractRedisConfiguration.RedisCommandLatencyRecorderConfiguration.class, Qualifiers.byName(config.getName()))
                                .orElse(defaultConfiguration);
        MicrometerOptions options = recorderConfiguration.toMicrometerOptions();
        builder.commandLatencyRecorder(new MicrometerCommandLatencyRecorder(meterRegistry, options));
    }
}
