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

import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.micronaut.context.env.Environment;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
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

    private RedisURI uri;
    private List<RedisURI> uris = Collections.emptyList();
    private List<RedisURI> replicaUris = Collections.emptyList();
    private Integer ioThreadPoolSize;
    private Integer computationThreadPoolSize;
    private String name;
    private ReadFrom readFrom;

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
        this.uris = Arrays.stream(uris).map(RedisURI::create).toList();
    }

    /**
     * @return Get the Redis URIs for read replicas.
     * @since 6.5.0
     */
    public List<RedisURI> getReplicaUris() {
        return replicaUris;
    }

    /**
     * Sets the Replica Redis URIs for read replica configuration.
     *
     * @param uris The URI
     * @since 6.5.0
     */
    public void setReplicaUris(@NonNull URI... uris) {
        this.replicaUris = Arrays.stream(uris).map(RedisURI::create).toList();
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

    /**
     * Lettuce command latency recorder settings.
     *
     * @since 6.7.0
     */
    public abstract static class RedisCommandLatencyRecorderConfiguration {
        private Boolean enabled;
        private Boolean histogram;
        private Boolean localDistinction;
        private Duration minLatency;
        private Duration maxLatency;
        private List<Double> targetPercentiles;

        /**
         * @return Whether the recorder is enabled.
         */
        public boolean isEnabled() {
            return enabled == null ? MicrometerOptions.DEFAULT_ENABLED : enabled;
        }

        /**
         * @param enabled Whether the recorder is enabled
         */
        public void setEnabled(@Nullable Boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * @return Whether histograms are enabled.
         */
        public boolean isHistogram() {
            return histogram == null ? true : histogram;
        }

        /**
         * @param histogram Whether histograms are enabled
         */
        public void setHistogram(@Nullable Boolean histogram) {
            this.histogram = histogram;
        }

        /**
         * @return Whether metrics are tracked per connection.
         */
        public boolean isLocalDistinction() {
            return localDistinction == null ? MicrometerOptions.DEFAULT_LOCAL_DISTINCTION : localDistinction;
        }

        /**
         * @param localDistinction Whether metrics are tracked per connection
         */
        public void setLocalDistinction(@Nullable Boolean localDistinction) {
            this.localDistinction = localDistinction;
        }

        /**
         * @return The minimum expected latency.
         */
        public Duration getMinLatency() {
            return minLatency == null ? MicrometerOptions.DEFAULT_MIN_LATENCY : minLatency;
        }

        /**
         * @param minLatency The minimum expected latency
         */
        public void setMinLatency(@Nullable Duration minLatency) {
            this.minLatency = minLatency;
        }

        /**
         * @return The maximum expected latency.
         */
        public Duration getMaxLatency() {
            return maxLatency == null ? MicrometerOptions.DEFAULT_MAX_LATENCY : maxLatency;
        }

        /**
         * @param maxLatency The maximum expected latency
         */
        public void setMaxLatency(@Nullable Duration maxLatency) {
            this.maxLatency = maxLatency;
        }

        /**
         * @return The target percentiles.
         */
        public double[] getTargetPercentiles() {
            if (targetPercentiles == null) {
                return MicrometerOptions.DEFAULT_TARGET_PERCENTILES;
            }
            return targetPercentiles.stream()
                    .mapToDouble(Double::doubleValue)
                    .toArray();
        }

        /**
         * @param targetPercentiles The target percentiles.
         */
        public void setTargetPercentiles(@Nullable List<Double> targetPercentiles) {
            this.targetPercentiles = targetPercentiles;
        }

        /**
         * @return The Micrometer options represented by this configuration.
         */
        public MicrometerOptions toMicrometerOptions() {
            MicrometerOptions.Builder builder = MicrometerOptions.builder()
                    .histogram(isHistogram())
                    .localDistinction(isLocalDistinction())
                    .minLatency(getMinLatency())
                    .maxLatency(getMaxLatency())
                    .targetPercentiles(getTargetPercentiles());
            if (!isEnabled()) {
                builder.disable();
            }
            return builder.build();
        }
    }

}
