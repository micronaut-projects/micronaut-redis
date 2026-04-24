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
package io.micronaut.configuration.lettuce.pubsub.bind;

import io.micronaut.configuration.lettuce.pubsub.RedisListenerMessage;
import io.micronaut.configuration.lettuce.pubsub.RedisMessageBodyHandler;
import io.micronaut.core.bind.ArgumentBinder;
import io.micronaut.core.convert.ArgumentConversionContext;
import jakarta.inject.Singleton;

import java.util.Optional;

/**
 * Binds the message body.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
public class RedisBodyBinder implements ArgumentBinder<Object, RedisListenerMessage> {

    private final RedisMessageBodyHandler messageBodyHandler;

    /**
     * @param messageBodyHandler The message body handler
     */
    public RedisBodyBinder(RedisMessageBodyHandler messageBodyHandler) {
        this.messageBodyHandler = messageBodyHandler;
    }

    @Override
    public BindingResult<Object> bind(ArgumentConversionContext<Object> context, RedisListenerMessage source) {
        Optional<Object> value = messageBodyHandler.deserialize(source.message().body(), context, source.mediaType());
        return () -> value;
    }
}
