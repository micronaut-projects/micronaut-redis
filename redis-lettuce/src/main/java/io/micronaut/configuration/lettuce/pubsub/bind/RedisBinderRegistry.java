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

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.core.bind.ArgumentBinder;
import io.micronaut.core.bind.ArgumentBinderRegistry;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArrayUtils;
import jakarta.inject.Singleton;

import java.lang.annotation.Annotation;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Used to determine which {@link RedisArgumentBinder} to use for any given argument.
 *
 * @author Cristian Morales
 * @since TODO
 */
@Singleton
public class RedisBinderRegistry implements ArgumentBinderRegistry<RedisMessage> {

    private final Map<Class<? extends Annotation>, ArgumentBinder<?, RedisMessage>> byAnnotation = new LinkedHashMap<>();
    private final Map<Integer, ArgumentBinder<?, RedisMessage>> byType = new LinkedHashMap<>();
    private final RedisArgumentBinder<?> defaultBinder;

    /**
     * Default constructor.
     *
     * @param defaultBinder The binder to use when one cannot be found for an argument
     * @param binders       The list of binders to choose from to bind an argument
     */
    public RedisBinderRegistry(RedisArgumentBinder<?> defaultBinder, RedisArgumentBinder<?>... binders) {
        this.defaultBinder = defaultBinder;
        if (ArrayUtils.isNotEmpty(binders)) {
            for (RedisArgumentBinder<?> binder : binders) {
                if (binder instanceof AnnotatedRedisMessageBinder) {
                    AnnotatedRedisMessageBinder<?, ?> annotatedBinder = (AnnotatedRedisMessageBinder<?, ?>) binder;
                    byAnnotation.put(annotatedBinder.annotationType(), binder);
                } else if (binder instanceof TypedRedisMessageBinder) {
                    TypedRedisMessageBinder<?> typedBinder = (TypedRedisMessageBinder<?>) binder;
                    byType.put(typedBinder.argumentType().typeHashCode(), typedBinder);
                }
            }
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Optional<ArgumentBinder<T, RedisMessage>> findArgumentBinder(Argument<T> argument, RedisMessage source) {
        Optional<Class<? extends Annotation>> opt = argument.getAnnotationMetadata().getAnnotationTypeByStereotype(Bindable.class);

        if (opt.isPresent()) {
            Class<? extends Annotation> annotationType = opt.get();
            ArgumentBinder binder = byAnnotation.get(annotationType);
            if (binder != null) {
                return Optional.of(binder);
            }
        } else {
            ArgumentBinder binder = byType.get(argument.typeHashCode());
            if (binder != null) {
                return Optional.of(binder);
            }
        }

        return Optional.of((ArgumentBinder<T, RedisMessage>) defaultBinder);
    }

}
