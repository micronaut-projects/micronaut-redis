package io.micronaut.configuration.lettuce.health

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.resource.ClientResources
import io.micronaut.configuration.lettuce.AbstractRedisConfiguration
import io.micronaut.configuration.lettuce.ClientResourcesMutator
import io.micronaut.configuration.lettuce.DefaultRedisClientFactory
import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.health.HealthStatus
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.redis.test.RedisContainerUtils
import jakarta.inject.Singleton
import org.jspecify.annotations.Nullable
import reactor.core.publisher.Flux

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author graemerocher
 * @since 1.0
 */
class RedisHealthIndicatorSpec extends RedisSpec {

    void "redis health indicator opens a new redis connection for repeated checks by default"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.port': RedisContainerUtils.getRedisPort(),
                'spec.name': CountingRedisClientFactory.SPEC_NAME
        ])
        RedisHealthIndicator healthIndicator = applicationContext.getBean(RedisHealthIndicator)
        ConnectCounter connectCounter = applicationContext.getBean(ConnectCounter)

        then:
        connectCounter.connectCalls == 0

        when:
        HealthResult first = Flux.from(healthIndicator.getResult()).blockFirst()
        int initialConnectCalls = connectCounter.connectCalls
        HealthResult second = Flux.from(healthIndicator.getResult()).blockFirst()
        HealthResult third = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        first != null
        first.status == HealthStatus.UP
        initialConnectCalls == 1
        second != null
        second.status == HealthStatus.UP
        third != null
        third.status == HealthStatus.UP
        connectCounter.connectCalls == 3

        cleanup:
        applicationContext.close()
    }

    void "redis health indicator can reuse existing redis connection for repeated checks"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.port': RedisContainerUtils.getRedisPort(),
                'redis.health.reuse-connection': true,
                'spec.name': CountingRedisClientFactory.SPEC_NAME
        ])
        RedisHealthIndicator healthIndicator = applicationContext.getBean(RedisHealthIndicator)
        ConnectCounter connectCounter = applicationContext.getBean(ConnectCounter)

        then:
        connectCounter.connectCalls == 0

        when:
        HealthResult first = Flux.from(healthIndicator.getResult()).blockFirst()
        int initialConnectCalls = connectCounter.connectCalls
        HealthResult second = Flux.from(healthIndicator.getResult()).blockFirst()
        HealthResult third = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        first != null
        first.status == HealthStatus.UP
        second != null
        second.status == HealthStatus.UP
        third != null
        third.status == HealthStatus.UP
        initialConnectCalls == 1
        connectCounter.connectCalls == initialConnectCalls

        cleanup:
        applicationContext.close()
    }

    void "test redis health indicator"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run('redis.port': RedisContainerUtils.getRedisPort())
        RedisClient client = applicationContext.getBean(RedisClient)

        then:
        client != null

        when:
        RedisHealthIndicator healthIndicator = applicationContext.getBean(RedisHealthIndicator)
        HealthResult result = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        result != null
        result.status == HealthStatus.UP

        when:
        RedisContainerUtils.stopRedis()
        result = Flux.from(healthIndicator.getResult()).blockFirst()

        then:
        result != null
        result.status == HealthStatus.DOWN

        cleanup:
        applicationContext.close()
    }

    void "redis health indicator is not loaded when disabled"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.health.enabled': 'false',
                'redis.port': RedisContainerUtils.getRedisPort()
        ])
        RedisClient client = applicationContext.getBean(RedisClient)

        then:
        client != null

        when:
        Optional<RedisHealthIndicator> healthIndicator = applicationContext.findBean(RedisHealthIndicator)

        then:
        !healthIndicator.isPresent()

        cleanup:
        applicationContext.close()
    }

    @Factory
    @Requires(property = "spec.name", value = SPEC_NAME)
    static class CountingRedisClientFactory {
        static final String SPEC_NAME = "redis-health-indicator-connection-reuse"

        @Singleton
        ConnectCounter connectCounter() {
            return new ConnectCounter()
        }

        @Singleton
        @Primary
        @Replaces(RedisClient)
        RedisClient redisClient(
                @Primary AbstractRedisConfiguration config,
                @Nullable @Primary ClientResources defaultClientResources,
                @Nullable List<ClientResourcesMutator> mutators,
                ConnectCounter connectCounter
        ) {
            RedisClient delegate = new DefaultRedisClientFactory<String, String>(StringCodec.ASCII)
                    .redisClient(config, defaultClientResources, mutators)
            return new CountingRedisClient(delegate, connectCounter)
        }
    }

    static final class ConnectCounter {
        private final AtomicInteger connectCalls = new AtomicInteger()

        int getConnectCalls() {
            return connectCalls.get()
        }

        void incrementConnectCalls() {
            connectCalls.incrementAndGet()
        }
    }

    static final class CountingRedisClient extends RedisClient {
        private final RedisClient delegate
        private final ConnectCounter connectCounter

        CountingRedisClient(RedisClient delegate, ConnectCounter connectCounter) {
            this.delegate = delegate
            this.connectCounter = connectCounter
        }

        @Override
        StatefulRedisConnection<String, String> connect() {
            connectCounter.incrementConnectCalls()
            return delegate.connect()
        }

        @Override
        <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec) {
            connectCounter.incrementConnectCalls()
            return delegate.connect(codec)
        }

        @Override
        StatefulRedisConnection<String, String> connect(RedisURI redisURI) {
            connectCounter.incrementConnectCalls()
            return delegate.connect(redisURI)
        }

        @Override
        <K, V> StatefulRedisConnection<K, V> connect(RedisCodec<K, V> codec, RedisURI redisURI) {
            connectCounter.incrementConnectCalls()
            return delegate.connect(codec, redisURI)
        }

        @Override
        StatefulRedisPubSubConnection<String, String> connectPubSub() {
            return delegate.connectPubSub()
        }

        @Override
        <K, V> StatefulRedisPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {
            return delegate.connectPubSub(codec)
        }

        @Override
        void shutdown() {
            delegate.shutdown()
        }

        @Override
        void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {
            delegate.shutdown(quietPeriod, timeout, timeUnit)
        }
    }
}
