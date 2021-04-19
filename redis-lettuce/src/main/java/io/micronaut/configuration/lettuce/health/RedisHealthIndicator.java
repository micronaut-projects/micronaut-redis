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
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import javax.inject.Singleton;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import static io.micronaut.configuration.lettuce.health.RedisHealthIndicator.NAME;

/**
 * A Health Indicator for Redis.
 *
 * @author graemerocher
 * @since 1.0
 */
@Singleton
@Requires(classes = HealthIndicator.class)
@Requires(property = NAME + ".health.enabled", defaultValue = StringUtils.TRUE, notEquals = StringUtils.FALSE)
public class RedisHealthIndicator implements HealthIndicator {
    public static final Logger LOG = LoggerFactory.getLogger(RedisHealthIndicator.class);
    /**
     * Default name to use for health indication for Redis.
     */
    public static final String NAME = "redis";

    private static final int TIMEOUT_SECONDS = 3;
    private static final int RETRY = 3;

    private final BeanContext beanContext;
    private final HealthAggregator<?> healthAggregator;

    // Must include the connections otherwise the health check will be unknown until the first Redis command executed
    private final RedisClient[] redisClients;
    private final RedisClusterClient[] redisClusterClients;

    /**
     * Constructor.
     *
     * @param beanContext         beanContext
     * @param healthAggregator    healthAggregator
     * @param redisClients        redisClients
     * @param redisClusterClients redisClusterClients
     */
    public RedisHealthIndicator(BeanContext beanContext, HealthAggregator<?> healthAggregator, RedisClient[] redisClients, RedisClusterClient[] redisClusterClients) {
        this.beanContext = beanContext;
        this.healthAggregator = healthAggregator;
        this.redisClients = redisClients;
        this.redisClusterClients = redisClusterClients;
    }

    @Override
    public Publisher<HealthResult> getResult() {
        Flux<HealthResult> clientResults = getResult(RedisClient.class, RedisClient::connect, StatefulRedisConnection::reactive);
        Flux<HealthResult> clusteredClientResults = getResult(RedisClusterClient.class, RedisClusterClient::connect, StatefulRedisClusterConnection::reactive);
        return this.healthAggregator.aggregate(
                NAME,
                Flux.concat(clientResults, clusteredClientResults)
        );
    }

    private <T, R extends StatefulConnection<K, V>, K, V> Flux<HealthResult> getResult(Class<T> type, Function<T, R> getConnection, Function<R, BaseRedisReactiveCommands<K, V>> getReactive) {
        Collection<BeanRegistration<T>> registrations = beanContext.getActiveBeanRegistrations(type);
        Flux<BeanRegistration<T>> redisClients = Flux.fromIterable(registrations);
        return redisClients.flatMap(client -> healthResultForClient(client, getConnection, getReactive));
    }

    private <T, R extends StatefulConnection<K, V>, K, V> Mono<HealthResult> healthResultForClient(BeanRegistration<T> client, Function<T, R> getConnection, Function<R, BaseRedisReactiveCommands<K, V>> getReactive) {
        R connection;
        String connectionName = client.getIdentifier().getName();
        String dbName = "redis(" + connectionName + ")";
        try {
            connection = getConnection.apply(client.getBean());
        } catch (Exception e) {
            return Mono.just(healthResultForThrowable(e, dbName));
        }
        Mono<String> pingCommand = getReactive.apply(connection).ping();
        pingCommand = pingCommand.timeout(Duration.ofSeconds(TIMEOUT_SECONDS)).retry(RETRY);
        return pingCommand.map(s -> healthResultForPingResponse(s, dbName))
                .onErrorResume(throwable -> Mono.just(healthResultForThrowable(throwable, dbName)))
                .doFinally(f -> closeOnSignal(connection, f));
    }

    private <R extends StatefulConnection<K, V>, K, V> void closeOnSignal(R connection, SignalType signalType) {
        try {
            LOG.trace("Closing connection on signal " + signalType);
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
        if (pingResponse.equalsIgnoreCase("pong")) {
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
