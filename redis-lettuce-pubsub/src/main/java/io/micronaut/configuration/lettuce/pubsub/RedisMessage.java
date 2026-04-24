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
package io.micronaut.configuration.lettuce.pubsub;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The message delivered from Redis Pub/Sub.
 *
 * @param body    The raw message body
 * @param channel The resolved channel name
 * @param pattern The subscription pattern if the message was matched by pattern
 * @author Graeme Rocher
 * @since 7.0
 */
public record RedisMessage(byte[] body, String channel, @Nullable String pattern) {

    public RedisMessage {
        body = body.clone();
    }

    /**
     * @return The raw message body
     */
    public byte[] getBody() {
        return body.clone();
    }

    /**
     * @return The channel name
     */
    public String getChannel() {
        return channel;
    }

    /**
     * @return The subscription pattern, if any
     */
    public Optional<String> getPattern() {
        return Optional.ofNullable(pattern);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RedisMessage other)) {
            return false;
        }
        return Arrays.equals(body, other.body)
            && Objects.equals(channel, other.channel)
            && Objects.equals(pattern, other.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(body), channel, pattern);
    }

    @Override
    public String toString() {
        return "RedisMessage[body=" + Arrays.toString(body) + ", channel=" + channel + ", pattern=" + pattern + ']';
    }
}
