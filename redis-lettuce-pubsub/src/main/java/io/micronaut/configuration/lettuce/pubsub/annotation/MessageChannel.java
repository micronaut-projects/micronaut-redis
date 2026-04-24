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
package io.micronaut.configuration.lettuce.pubsub.annotation;

import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler;
import io.micronaut.context.annotation.Executable;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.messaging.annotation.MessageMapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Declares the Redis Pub/Sub channel or pattern to listen to, or binds the resolved channel on a method argument.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Documented
@Retention(RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
@Bindable
@Inherited
@Executable(processOnStartup = true)
@MessageMapping
public @interface MessageChannel {

    /**
     * @return The primary channel name
     */
    String value() default "";

    /**
     * @return The explicit channels to subscribe to
     */
    String[] channels() default {};

    /**
     * @return The explicit patterns to subscribe to
     */
    String[] patterns() default {};

    /**
     * @return The exception handler to use for this subscription
     */
    Class<? extends RedisListenerExceptionHandler> exceptionHandler() default RedisListenerExceptionHandler.class;
}
