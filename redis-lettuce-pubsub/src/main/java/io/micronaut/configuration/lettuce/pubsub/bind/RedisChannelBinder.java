/*
 * Copyright 2017-2026 original authors
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
package io.micronaut.configuration.lettuce.pubsub.bind;

import io.micronaut.configuration.lettuce.pubsub.RedisListenerMessage;
import io.micronaut.core.bind.ArgumentBinder;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import jakarta.inject.Singleton;

import java.util.Optional;

/**
 * Binds the resolved Redis Pub/Sub channel.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
public class RedisChannelBinder implements ArgumentBinder<Object, RedisListenerMessage> {

    private final ConversionService conversionService;

    /**
     * @param conversionService The conversion service
     */
    public RedisChannelBinder(ConversionService conversionService) {
        this.conversionService = conversionService;
    }

    @Override
    public BindingResult<Object> bind(ArgumentConversionContext<Object> context, RedisListenerMessage source) {
        Optional<Object> value = conversionService.convert(source.message().channel(), context);
        return () -> value;
    }
}
