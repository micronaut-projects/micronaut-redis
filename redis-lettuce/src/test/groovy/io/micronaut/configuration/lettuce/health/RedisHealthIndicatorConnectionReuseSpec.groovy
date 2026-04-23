package io.micronaut.configuration.lettuce.health

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.micronaut.context.BeanContext
import io.micronaut.context.BeanRegistration
import io.micronaut.health.HealthStatus
import io.micronaut.inject.BeanIdentifier
import io.micronaut.management.health.aggregator.HealthAggregator
import io.micronaut.management.health.indicator.HealthResult
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class RedisHealthIndicatorConnectionReuseSpec extends Specification {

    void "health indicator opens a new redis connection by default"() {
        given:
        BeanContext beanContext = Mock()
        HealthAggregator<HealthResult> healthAggregator = Mock()
        RedisClient redisClient = Mock()
        AtomicInteger closeCalls = new AtomicInteger()
        StatefulRedisConnection<Object, Object> redisConnection = Mock() {
            close() >> { closeCalls.incrementAndGet() }
        }
        RedisReactiveCommands<Object, Object> reactiveCommands = Mock()
        BeanRegistration<RedisClient> registration = Stub() {
            getBean() >> redisClient
            getIdentifier() >> Stub(BeanIdentifier) {
                getName() >> "default"
            }
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor()
        beanContext.findBean(RedisHealthIndicatorConfiguration) >> Optional.empty()
        RedisHealthIndicator healthIndicator = new RedisHealthIndicator(beanContext, executorService, healthAggregator, [redisClient] as RedisClient[], [] as io.lettuce.core.cluster.RedisClusterClient[])
        beanContext.getActiveBeanRegistrations(RedisClient) >> [registration]
        beanContext.getActiveBeanRegistrations(io.lettuce.core.cluster.RedisClusterClient) >> []
        healthAggregator.aggregate(RedisHealthIndicator.NAME, _) >> { String name, publisher -> publisher }
        redisClient.connect() >> redisConnection
        redisConnection.reactive() >> reactiveCommands
        reactiveCommands.ping() >> Mono.just("PONG")

        when:
        HealthResult result = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        result != null
        result.status == HealthStatus.UP
        new PollingConditions(timeout: 1).eventually {
            assert closeCalls.get() == 1
        }

        cleanup:
        executorService.shutdownNow()
    }

    void "health indicator reuses an existing redis connection bean when configured"() {
        given:
        BeanContext beanContext = Mock()
        HealthAggregator<HealthResult> healthAggregator = Mock()
        RedisClient redisClient = Mock()
        RedisHealthIndicatorConfiguration configuration = new RedisHealthIndicatorConfiguration()
        configuration.reuseConnection = true
        StatefulRedisConnection<Object, Object> redisConnection = Mock()
        RedisReactiveCommands<Object, Object> reactiveCommands = Mock()
        BeanRegistration<RedisClient> registration = Stub() {
            getBean() >> redisClient
            getIdentifier() >> Stub(BeanIdentifier) {
                getName() >> "default"
            }
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor()
        beanContext.findBean(RedisHealthIndicatorConfiguration) >> Optional.of(configuration)
        RedisHealthIndicator healthIndicator = new RedisHealthIndicator(beanContext, executorService, healthAggregator, [redisClient] as RedisClient[], [] as io.lettuce.core.cluster.RedisClusterClient[])
        beanContext.getActiveBeanRegistrations(RedisClient) >> [registration]
        beanContext.getActiveBeanRegistrations(io.lettuce.core.cluster.RedisClusterClient) >> []
        beanContext.findBean(StatefulRedisConnection, _) >> Optional.of(redisConnection)
        healthAggregator.aggregate(RedisHealthIndicator.NAME, _) >> { String name, publisher -> publisher }
        redisConnection.reactive() >> reactiveCommands
        reactiveCommands.ping() >> Mono.just("PONG")

        when:
        HealthResult result = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        result != null
        result.status == HealthStatus.UP
        0 * redisClient.connect()
        0 * redisConnection.close()

        cleanup:
        executorService.shutdownNow()
    }

    void "health indicator falls back to opening a redis connection when reuse is enabled and none exists"() {
        given:
        BeanContext beanContext = Mock()
        HealthAggregator<HealthResult> healthAggregator = Mock()
        RedisClient redisClient = Mock()
        RedisHealthIndicatorConfiguration configuration = new RedisHealthIndicatorConfiguration()
        configuration.reuseConnection = true
        AtomicInteger closeCalls = new AtomicInteger()
        StatefulRedisConnection<Object, Object> redisConnection = Mock() {
            close() >> { closeCalls.incrementAndGet() }
        }
        RedisReactiveCommands<Object, Object> reactiveCommands = Mock()
        BeanRegistration<RedisClient> registration = Stub() {
            getBean() >> redisClient
            getIdentifier() >> Stub(BeanIdentifier) {
                getName() >> "default"
            }
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor()
        beanContext.findBean(RedisHealthIndicatorConfiguration) >> Optional.of(configuration)
        RedisHealthIndicator healthIndicator = new RedisHealthIndicator(beanContext, executorService, healthAggregator, [redisClient] as RedisClient[], [] as io.lettuce.core.cluster.RedisClusterClient[])
        beanContext.getActiveBeanRegistrations(RedisClient) >> [registration]
        beanContext.getActiveBeanRegistrations(io.lettuce.core.cluster.RedisClusterClient) >> []
        beanContext.findBean(StatefulRedisConnection, _) >> Optional.empty()
        beanContext.findBean(StatefulRedisConnection) >> Optional.empty()
        healthAggregator.aggregate(RedisHealthIndicator.NAME, _) >> { String name, publisher -> publisher }
        redisClient.connect() >> redisConnection
        redisConnection.reactive() >> reactiveCommands
        reactiveCommands.ping() >> Mono.just("PONG")

        when:
        HealthResult result = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        result != null
        result.status == HealthStatus.UP
        new PollingConditions(timeout: 1).eventually {
            assert closeCalls.get() == 1
        }

        cleanup:
        executorService.shutdownNow()
    }
}
