/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce.cache;

import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.runtime.ApplicationConfiguration;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;

/**
 * Allows configuration of caches stored in Redis.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@EachProperty(RedisSetting.REDIS_CACHES)
public class RedisCacheConfiguration {

    protected String server;
    protected Class<ObjectSerializer> keySerializer;
    protected Class<ObjectSerializer> valueSerializer;
    protected Charset charset;
    protected Duration expireAfterWrite;
    protected Duration expireAfterAccess;
    protected final String cacheName;

    /**
     * Constructor.
     * @param cacheName cacheName
     * @param applicationConfiguration applicationConfiguration
     */
    public RedisCacheConfiguration(@Parameter String cacheName, ApplicationConfiguration applicationConfiguration) {
        this.cacheName = cacheName;
        this.charset = applicationConfiguration.getDefaultCharset();
    }

    /**
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
     * @return The name of the server to use.
     */
    public Optional<String> getServer() {
        if (server != null) {
            return Optional.of(server);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The {@link ObjectSerializer} type to use for serializing values.
     */
    public Optional<Class<ObjectSerializer>> getValueSerializer() {
        return Optional.ofNullable(valueSerializer);
    }

    /**
     * The {@link ObjectSerializer} to use for serializing keys. Defaults to {@link io.micronaut.cache.serialize.DefaultStringKeySerializer}.
     *
     * @return The optional {@link ObjectSerializer} class
     */
    public Optional<Class<ObjectSerializer>> getKeySerializer() {
        return Optional.ofNullable(keySerializer);
    }

    /**
     * @return The name of the cache
     */
    public String getCacheName() {
        return cacheName;
    }


    /**
     * @return The expiry to use after the value is written
     */
    public Optional<Duration> getExpireAfterWrite() {
        return Optional.ofNullable(expireAfterWrite);
    }

    /**
     * Specifies that each entry should be automatically removed from the cache once a fixed duration
     * has elapsed after the entry's creation, the most recent replacement of its value, or its last
     * read.
     *
     * @return The {@link Duration}
     */
    public Optional<Duration> getExpireAfterAccess() {
        return Optional.ofNullable(expireAfterAccess);
    }

    /**
     * @return The charset used to serialize and deserialize values
     */
    public Charset getCharset() {
        return charset;
    }

    /**
     *
     * @param expireAfterWrite The cache expiration duration after writing into it.
     */
    public void setExpireAfterWrite(Duration expireAfterWrite) {
        this.expireAfterWrite = expireAfterWrite;
    }

    /**
     * @param expireAfterAccess The cache expiration duration after accessing it
     */
    public void setExpireAfterAccess(Duration expireAfterAccess) {
        this.expireAfterAccess = expireAfterAccess;
    }

    /**
     * @param charset The charset used to serialize and deserialize values
     */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }
}
