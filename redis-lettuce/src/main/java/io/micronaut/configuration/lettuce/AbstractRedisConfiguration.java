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
package io.micronaut.configuration.lettuce;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.micronaut.context.env.Environment;
import org.jspecify.annotations.NonNull;
import io.micronaut.core.naming.Named;
import io.micronaut.core.util.Toggleable;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Abstract configuration for Lettuce.
 */
public abstract class AbstractRedisConfiguration extends RedisURI implements Named, Toggleable {

    private RedisURI rawUri;
    private List<RedisURI> rawUris = Collections.emptyList();
    private List<RedisURI> rawReplicaUris = Collections.emptyList();
    private volatile RedisURI mergedUri;
    private volatile List<RedisURI> mergedUris = Collections.emptyList();
    private volatile List<RedisURI> mergedReplicaUris = Collections.emptyList();
    private Integer ioThreadPoolSize;
    private Integer computationThreadPoolSize;
    private String name;
    private ReadFrom readFrom;
    private Duration configuredTimeout;
    private Integer configuredDatabase;
    private Boolean configuredSsl;
    private Boolean configuredStartTls;
    private SslVerifyMode configuredVerifyMode;

    /**
     * Constructor.
     */
    protected AbstractRedisConfiguration() {
        setName(Environment.DEFAULT_NAME);
        setPort(RedisURI.DEFAULT_REDIS_PORT);
        setHost("localhost"); // localhost by default
    }

    /**
     * @return Get the Redis URI for configuration.
     */
    public Optional<RedisURI> getUri() {
        return Optional.ofNullable(mergedUri);
    }

    /**
     * Sets the Redis URI for configuration by string.
     *
     * @param uri The URI
     */
    public void setUri(URI uri) {
        this.rawUri = RedisURI.create(uri);
        recomputeMergedUris();
    }

    /**
     * @return Get the Redis URIs for cluster configuration.
     */
    public List<RedisURI> getUris() {
        return mergedUris;
    }

    /**
     * Sets the Redis URIs for cluster configuration.
     *
     * @param uris The URI
     */
    public void setUris(URI... uris) {
        this.rawUris = Arrays.stream(uris).map(RedisURI::create).toList();
        recomputeMergedUris();
    }

    /**
     * @return Get the Redis URIs for read replicas.
     * @since 6.5.0
     */
    public List<RedisURI> getReplicaUris() {
        return mergedReplicaUris;
    }

    /**
     * Sets the Replica Redis URIs for read replica configuration.
     *
     * @param uris The URI
     * @since 6.5.0
     */
    public void setReplicaUris(@NonNull URI... uris) {
        this.rawReplicaUris = Arrays.stream(uris).map(RedisURI::create).toList();
        recomputeMergedUris();
    }

    /**
     * Returns the pool size (number of threads) for IO threads. The indicated size does not reflect the number for all IO
     * threads. TCP and socket connections (epoll) require different IO pool.
     *
     * {@link io.lettuce.core.resource.ClientResources#ioThreadPoolSize()}
     *
     * @return the pool size (number of threads) for all IO tasks.
     */
    public Integer getIoThreadPoolSize() {
        return ioThreadPoolSize;
    }

    /**
     * Sets the thread pool size (number of threads to use) for I/O operations (default value is the number of CPUs). The
     * thread pool size is only effective if no {@link io.lettuce.core.resource.ClientResources.Builder#eventLoopGroupProvider} is provided.
     *
     * {@link io.lettuce.core.resource.ClientResources.Builder#ioThreadPoolSize(int)}
     *
     * @param ioThreadPoolSize the thread pool size, must be greater {@code 0}.
     */
    public void setIoThreadPoolSize(Integer ioThreadPoolSize) {
        this.ioThreadPoolSize = ioThreadPoolSize;
    }

    /**
     * Returns the pool size (number of threads) for all computation tasks.
     *
     * {@link io.lettuce.core.resource.ClientResources#computationThreadPoolSize()}
     *
     * @return the pool size (number of threads to use).
     */
    public Integer getComputationThreadPoolSize() {
        return computationThreadPoolSize;
    }

    /**
     * Sets the thread pool size (number of threads to use) for computation operations (default value is the number of
     * CPUs). The thread pool size is only effective if no {@link io.lettuce.core.resource.ClientResources.Builder#eventExecutorGroup} is provided.
     *
     * {@link io.lettuce.core.resource.ClientResources.Builder#computationThreadPoolSize(int)}
     *
     * @param computationThreadPoolSize the thread pool size, must be greater {@code 0}.
     */
    public void setComputationThreadPoolSize(Integer computationThreadPoolSize) {
        this.computationThreadPoolSize = computationThreadPoolSize;
    }

    /**
     * @return Get the name of the bean.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the bean.
     *
     * @param name The name of the bean
     */
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setTimeout(Duration timeout) {
        super.setTimeout(timeout);
        this.configuredTimeout = timeout;
        recomputeMergedUris();
    }

    @Override
    public void setDatabase(int database) {
        super.setDatabase(database);
        this.configuredDatabase = database;
        recomputeMergedUris();
    }

    @Override
    public void setSsl(boolean ssl) {
        super.setSsl(ssl);
        this.configuredSsl = ssl;
        recomputeMergedUris();
    }

    @Override
    public void setStartTls(boolean startTls) {
        super.setStartTls(startTls);
        this.configuredStartTls = startTls;
        recomputeMergedUris();
    }

    @Override
    public void setVerifyPeer(boolean verifyPeer) {
        super.setVerifyPeer(verifyPeer);
        this.configuredVerifyMode = getVerifyMode();
        recomputeMergedUris();
    }

    @Override
    public void setVerifyPeer(SslVerifyMode verifyMode) {
        super.setVerifyPeer(verifyMode);
        this.configuredVerifyMode = verifyMode;
        recomputeMergedUris();
    }

    @Override
    public void setClientName(String clientName) {
        super.setClientName(clientName);
        recomputeMergedUris();
    }

    /**
     *
     * See {@link io.lettuce.core.ReadFrom}.
     *
     * @return Get the ReadFrom settings for the read replicas.
     * @since 6.5.0
     */
    public Optional<ReadFrom> getReadFrom() {
        return Optional.ofNullable(readFrom);
    }

    /**
     * Sets the read from property by name.
     *
     * See {@link io.lettuce.core.ReadFrom#valueOf(String)}
     *
     * @param readFrom The value of the ReadFrom setting to use.
     * @since 6.5.0
     */
    public void setReadFrom(@NonNull String readFrom) {
        this.readFrom = ReadFrom.valueOf(readFrom);
    }

    private void recomputeMergedUris() {
        this.mergedUri = rawUri == null ? null : applyConfiguredRedisUriSettings(rawUri);
        this.mergedUris = rawUris.stream().map(this::applyConfiguredRedisUriSettings).toList();
        this.mergedReplicaUris = rawReplicaUris.stream().map(this::applyConfiguredRedisUriSettings).toList();
    }

    private RedisURI applyConfiguredRedisUriSettings(RedisURI redisURI) {
        RedisURI.Builder builder = RedisURI.builder(redisURI);
        if (configuredTimeout != null) {
            builder.withTimeout(configuredTimeout);
        }
        if (configuredDatabase != null) {
            builder.withDatabase(configuredDatabase);
        }
        if (getClientName() != null) {
            builder.withClientName(getClientName());
        }
        if (configuredSsl != null) {
            builder.withSsl(configuredSsl);
        }
        if (configuredStartTls != null) {
            builder.withStartTls(configuredStartTls);
        }
        if (configuredVerifyMode != null) {
            builder.withVerifyPeer(configuredVerifyMode);
        }
        return builder.build();
    }
}
