/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.configuration.lettuce.health;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.async.publisher.AsyncSingleResultPublisher;
import io.micronaut.core.util.StringUtils;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * A Health Indicator for Redis.
 *
 * @author graemerocher
 * @since 1.0
 */
@Singleton
@Requires(classes = HealthIndicator.class)
@Requires(property = RedisHealthIndicator.NAME + ".health.enabled", defaultValue = StringUtils.TRUE, notEquals = StringUtils.FALSE)
public class RedisHealthIndicator implements HealthIndicator {
    public static final Logger LOG = LoggerFactory.getLogger(RedisHealthIndicator.class);
    /**
     * Default name to use for health indication for Redis.
     */
    public static final String NAME = "redis";
    public static final String PING_RESPONSE = "pong";
    private static final int TIMEOUT_SECONDS = 3;
    private static final int RETRY = 3;

    private final BeanContext beanContext;
    private final ExecutorService executorService;
    private final HealthAggregator<?> healthAggregator;

    // Must include the connections otherwise the health check will be unknown until the first Redis command executed
    private final RedisClient[] redisClients;
    private final RedisClusterClient[] redisClusterClients;

    /**
     * Constructor.
     *
     * @param beanContext beanContext
     * @param executorService executor service
     * @param healthAggregator healthAggregator
     * @param redisClients redisClients
     * @param redisClusterClients redisClusterClients
     */
    public RedisHealthIndicator(BeanContext beanContext,
                                @Named(TaskExecutors.IO) ExecutorService executorService,
                                HealthAggregator<?> healthAggregator, RedisClient[] redisClients,
                                RedisClusterClient[] redisClusterClients) {
        this.beanContext = beanContext;
        this.executorService = executorService;
        this.healthAggregator = healthAggregator;
        this.redisClients = redisClients;
        this.redisClusterClients = redisClusterClients;
    }

    @Override
    public Publisher<HealthResult> getResult() {
        Flux<HealthResult> clientResults = getResult(RedisClient.class, RedisClient::connect, StatefulRedisConnection::sync);
        Flux<HealthResult> clusteredClientResults = getResult(RedisClusterClient.class, RedisClusterClient::connect, StatefulRedisClusterConnection::sync);
        return this.healthAggregator.aggregate(
                NAME,
                Flux.concat(clientResults, clusteredClientResults)
        );
    }

    private <T, R extends StatefulConnection<K, V>, K, V> Flux<HealthResult> getResult(Class<T> type, Function<T, R> getConnection, Function<R, BaseRedisCommands<K, V>> getSync) {
        Collection<BeanRegistration<T>> registrations = beanContext.getActiveBeanRegistrations(type);
        Flux<BeanRegistration<T>> redisClients = Flux.fromIterable(registrations);
        return redisClients.flatMap(client -> healthResultForClient(client, getConnection, getSync));
    }

    private <T, R extends StatefulConnection<K, V>, K, V> Publisher<HealthResult> healthResultForClient(BeanRegistration<T> client, Function<T, R> getConnection, Function<R, BaseRedisCommands<K, V>> getSync) {
        if (executorService == null) {
            throw new IllegalStateException("I/O ExecutorService is null");
        }
        return new AsyncSingleResultPublisher<>(executorService, () -> {
            R connection = null;
            String connectionName = client.getIdentifier().getName();
            String dbName = "redis(" + connectionName + ")";
            try {
                connection = getConnection.apply(client.getBean());
                connection.setTimeout(Duration.ofSeconds(TIMEOUT_SECONDS));
                String pingResponse;
                int pingAttempt = 0;
                while (true) {
                    try {
                        pingResponse = getSync.apply(connection).ping();
                        break;
                    } catch (Exception e) {
                        LOG.error("Error executing ping command: ", e);
                        if (++pingAttempt == RETRY) {
                            return healthResultForThrowable(e, dbName);
                        }
                    }
                }
                return healthResultForPingResponse(pingResponse, dbName);
            } catch (Exception e) {
                return healthResultForThrowable(e, dbName);
            } finally {
                if (connection != null) {
                    close(connection);
                }
            }
        });
    }

    private <R extends StatefulConnection<K, V>, K, V> void close(R connection) {
        try {
            LOG.trace("Closing connection");
            connection.close();
        } catch (Exception e) {
            LOG.error("Failed to close connection", e);
        }
    }

    private HealthResult healthResultForThrowable(Throwable throwable, String name) {
        return HealthResult
                .builder(name, HealthStatus.DOWN)
                .exception(throwable)
                .build();
    }

    private HealthResult healthResultForPingResponse(String pingResponse, String name) {
        if (PING_RESPONSE.equalsIgnoreCase(pingResponse)) {
            return HealthResult
                    .builder(name, HealthStatus.UP)
                    .build();
        }
        return HealthResult
                .builder(name, HealthStatus.DOWN)
                .details(Collections.singletonMap("message", "Unexpected response: " + pingResponse))
                .build();
    }
}
