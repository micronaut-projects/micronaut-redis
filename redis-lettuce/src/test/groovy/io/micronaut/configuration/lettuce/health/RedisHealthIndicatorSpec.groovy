package io.micronaut.configuration.lettuce.health

import io.lettuce.core.RedisClient
import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.health.HealthStatus
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.redis.test.RedisContainerUtils
import reactor.core.publisher.Flux
import spock.lang.Specification

/**
 * @author graemerocher
 * @since 1.0
 */
class RedisHealthIndicatorSpec extends RedisSpec {

    private static String MAX_HEAP_SETTING = "maxmemory 256M"

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
}
