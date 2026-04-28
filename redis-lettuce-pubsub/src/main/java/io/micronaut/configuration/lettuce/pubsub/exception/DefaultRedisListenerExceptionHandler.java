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
package io.micronaut.configuration.lettuce.pubsub.exception;

import io.micronaut.context.annotation.Primary;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Redis listener exception handler that logs listener failures.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
@Primary
public class DefaultRedisListenerExceptionHandler implements RedisListenerExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRedisListenerExceptionHandler.class);

    @Override
    public void handle(RedisListenerException exception) {
        if (LOG.isErrorEnabled()) {
            exception.getRedisMessage().ifPresentOrElse(message ->
                    LOG.error(
                        "Error processing Redis Pub/Sub message on configured channel [{}] for resolved channel [{}] with listener [{}]: {}",
                        exception.getMessageChannel(),
                        message.getChannel(),
                        exception.getRedisListener(),
                        exception.getMessage(),
                        exception
                    ),
                () -> LOG.error(
                    "Error processing Redis Pub/Sub listener [{}] on configured channel [{}]: {}",
                    exception.getRedisListener(),
                    exception.getMessageChannel(),
                    exception.getMessage(),
                    exception
                )
            );
        }
    }
}
