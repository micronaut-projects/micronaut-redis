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
package io.micronaut.configuration.lettuce.pubsub.annotation;

import io.micronaut.aop.Introduction;
import io.micronaut.context.annotation.AliasFor;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Singleton;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Declarative client for publishing Redis Pub/Sub messages.
 * Methods may return a synchronous value, {@code void}, a {@link java.util.concurrent.CompletionStage},
 * or a reactive {@link org.reactivestreams.Publisher}. For non-void return types, Micronaut first tries
 * to convert the Redis subscriber count to the declared return type. If that is not possible and the
 * declared type already matches the published body type, the original body is returned.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Documented
@Retention(RUNTIME)
@Target(ElementType.TYPE)
@Introduction
@Singleton
public @interface RedisPubSubClient {

    /**
     * @return The named Redis connection
     */
    @AliasFor(member = "connection")
    String value() default "";

    /**
     * @return The named Redis connection
     */
    @AliasFor(member = "value")
    String connection() default "";

    /**
     * @return The executor used for asynchronous client methods
     */
    String executor() default TaskExecutors.BLOCKING;
}
