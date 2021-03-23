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

import io.lettuce.core.RedisURI;
import io.micronaut.context.env.Environment;
import io.micronaut.core.naming.Named;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstract configuration for Lettuce.
 */
public abstract class AbstractRedisConfiguration extends RedisURI implements Named {

    private RedisURI uri;
    private List<RedisURI> uris = Collections.emptyList();
    private Integer ioThreadPoolSize;
    private Integer computationThreadPoolSize;
    private String name;

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
        if (uri != null) {
            uri.setClientName(getClientName());
        }
        return Optional.ofNullable(uri);
    }

    /**
     * Sets the Redis URI for configuration by string.
     *
     * @param uri The URI
     */
    public void setUri(URI uri) {
        this.uri = RedisURI.create(uri);
    }

    /**
     * @return Get the Redis URIs for cluster configuration.
     */
    public List<RedisURI> getUris() {
        return uris;
    }

    /**
     * Sets the Redis URIs for cluster configuration.
     *
     * @param uris The URI
     */
    public void setUris(URI... uris) {
        this.uris = Arrays.stream(uris).map(RedisURI::create).collect(Collectors.toList());
    }

    /**
     * Returns the pool size (number of threads) for IO threads. The indicated size does not reflect the number for all IO
     * threads. TCP and socket connections (epoll) require different IO pool.
     *
     * {@link ClientResources#ioThreadPoolSize()}
     *
     * @return the pool size (number of threads) for all IO tasks.
     */
    public Integer getIoThreadPoolSize() {
        return ioThreadPoolSize;
    }

    /**
     * Sets the thread pool size (number of threads to use) for I/O operations (default value is the number of CPUs). The
     * thread pool size is only effective if no {@link ClientResources.Builder#eventLoopGroupProvider} is provided.
     *
     * {@link ClientResources.Builder#ioThreadPoolSize(int)}
     *
     * @param ioThreadPoolSize the thread pool size, must be greater {@code 0}.
     */
    public void setIoThreadPoolSize(Integer ioThreadPoolSize) {
        this.ioThreadPoolSize = ioThreadPoolSize;
    }

    /**
     * Returns the pool size (number of threads) for all computation tasks.
     *
     * {@link ClientResources#computationThreadPoolSize()}
     *
     * @return the pool size (number of threads to use).
     */
    public Integer getComputationThreadPoolSize() {
        return computationThreadPoolSize;
    }

    /**
     * Sets the thread pool size (number of threads to use) for computation operations (default value is the number of
     * CPUs). The thread pool size is only effective if no {@link ClientResources.Builder#eventExecutorGroup} is provided.
     *
     * {@link ClientResources.Builder#computationThreadPoolSize(int)}
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
}
