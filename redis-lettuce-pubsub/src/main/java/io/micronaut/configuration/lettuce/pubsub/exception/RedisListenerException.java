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
package io.micronaut.configuration.lettuce.pubsub.exception;

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.messaging.exceptions.MessageListenerException;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * Exception thrown when an error occurs processing a Redis Pub/Sub message.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
public class RedisListenerException extends MessageListenerException {

    private final transient Object listener;
    private final transient RedisMessage redisMessage;
    private final transient String channel;

    /**
     * @param message     The message
     * @param cause       The cause
     * @param listener    The listener bean
     * @param redisMessage The Redis message
     * @param channel     The configured message channel
     */
    public RedisListenerException(String message,
                                  Throwable cause,
                                  Object listener,
                                  @Nullable RedisMessage redisMessage,
                                  String channel) {
        super(message, cause);
        this.listener = listener;
        this.redisMessage = redisMessage;
        this.channel = channel;
    }

    /**
     * @return The listener bean
     */
    public Object getRedisListener() {
        return listener;
    }

    /**
     * @return The Redis message, if available
     */
    public Optional<RedisMessage> getRedisMessage() {
        return Optional.ofNullable(redisMessage);
    }

    /**
     * @return The configured message channel
     */
    public String getMessageChannel() {
        return channel;
    }
}
