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

import io.micronaut.context.annotation.AliasFor;
import io.micronaut.messaging.annotation.MessageListener;
import io.micronaut.scheduling.TaskExecutors;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a bean as a Redis Pub/Sub message listener.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Documented
@Retention(RUNTIME)
@Target(ElementType.TYPE)
@MessageListener
@Inherited
public @interface RedisListener {

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
     * @return The executor used to invoke listener methods
     */
    String executor() default TaskExecutors.BLOCKING;
}
