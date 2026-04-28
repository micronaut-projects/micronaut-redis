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

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.configuration.lettuce.pubsub.RedisListenerMessage;
import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel;
import io.micronaut.core.bind.ArgumentBinder;
import io.micronaut.core.bind.ArgumentBinderRegistry;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.type.Argument;
import io.micronaut.messaging.annotation.MessageBody;
import jakarta.inject.Singleton;

import java.lang.annotation.Annotation;
import java.util.Optional;

/**
 * Resolves binders for Redis Pub/Sub listener arguments.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
public class RedisBinderRegistry implements ArgumentBinderRegistry<RedisListenerMessage> {

    private final RedisBodyBinder bodyBinder;
    private final RedisChannelBinder channelBinder;
    private final RedisMessageBinder messageBinder;

    /**
     * @param bodyBinder    The body binder
     * @param channelBinder The channel binder
     * @param messageBinder The full message binder
     */
    public RedisBinderRegistry(RedisBodyBinder bodyBinder,
                               RedisChannelBinder channelBinder,
                               RedisMessageBinder messageBinder) {
        this.bodyBinder = bodyBinder;
        this.channelBinder = channelBinder;
        this.messageBinder = messageBinder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<ArgumentBinder<T, RedisListenerMessage>> findArgumentBinder(Argument<T> argument) {
        Optional<Class<? extends Annotation>> annotationType = argument.getAnnotationMetadata()
            .getAnnotationTypeByStereotype(Bindable.class);
        if (annotationType.isPresent()) {
            Class<? extends Annotation> annotation = annotationType.get();
            if (annotation == MessageBody.class) {
                return Optional.of((ArgumentBinder<T, RedisListenerMessage>) bodyBinder);
            }
            if (annotation == MessageChannel.class) {
                return Optional.of((ArgumentBinder<T, RedisListenerMessage>) channelBinder);
            }
        }
        if (RedisMessage.class.isAssignableFrom(argument.getType())) {
            return Optional.of((ArgumentBinder<T, RedisListenerMessage>) messageBinder);
        }
        return Optional.of((ArgumentBinder<T, RedisListenerMessage>) bodyBinder);
    }

    @Override
    @SuppressWarnings("removal")
    public <T> Optional<ArgumentBinder<T, RedisListenerMessage>> findArgumentBinder(Argument<T> argument, RedisListenerMessage source) {
        return findArgumentBinder(argument);
    }
}
