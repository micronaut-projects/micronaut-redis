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
package io.micronaut.configuration.lettuce.cache;

import io.lettuce.core.ReadFrom;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.runtime.ApplicationConfiguration;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;

/**
 * Allows configuration of caches stored in Redis.
 *
 * @author Graeme Rocher, Alex Katlein
 * @since 1.3
 */
public abstract class AbstractRedisCacheConfiguration {

    protected String server;
    protected Class<ObjectSerializer> keySerializer;
    protected Class<ObjectSerializer> valueSerializer;
    protected Charset charset;
    protected Duration expireAfterWrite;
    protected Duration expireAfterAccess;
    protected String expirationAfterWritePolicy;

    /**
     * Constructor.
     *
     * @param applicationConfiguration applicationConfiguration
     */
    public AbstractRedisCacheConfiguration(ApplicationConfiguration applicationConfiguration) {
        this.charset = applicationConfiguration.getDefaultCharset();
    }

    /**
     * @return The name of the server to use.
     * @see io.micronaut.configuration.lettuce.NamedRedisServersConfiguration
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
     * @return The class path for an implementation of ExpirationAfterWritePolicy
     */
    public Optional<String> getExpirationAfterWritePolicy() {
        return Optional.ofNullable(expirationAfterWritePolicy);
    }

    /**
     * @return The charset used to serialize and deserialize values
     */
    public Charset getCharset() {
        return charset;
    }

    /**
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

    /**
     * @param expirationAfterWritePolicy The class path for an implementation of ExpirationAfterWritePolicy
     */
    public void setExpirationAfterWritePolicy(String expirationAfterWritePolicy) {
        this.expirationAfterWritePolicy = expirationAfterWritePolicy;
    }
}
