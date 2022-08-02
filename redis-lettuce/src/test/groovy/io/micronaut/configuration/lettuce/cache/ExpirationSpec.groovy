package io.micronaut.configuration.lettuce.cache

import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.redis.test.RedisContainerUtils
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class ExpirationSpec extends RedisSpec {

    @Shared
    @AutoCleanup
    ApplicationContext applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'redis.caches.test1.enabled': 'true',
            'redis.caches.test1.expire-after-write': '1s',
            'redis.caches.test2.enabled': 'true',
            'redis.caches.test2.expiration-after-write-policy': 'io.micronaut.configuration.lettuce.cache.TestExpirationPolicy'
    )

    void "test constant-expiration-after-write-policy expires after set timeout"() {
        when:
        TimeService timeService = applicationContext.getBean(TimeService)
        long result = timeService.getTimeWithConstantExpirationPolicy()

        then:
        timeService.getTimeWithConstantExpirationPolicy() == result

        when:
        Thread.sleep(1000)

        then:
        timeService.getTimeWithConstantExpirationPolicy() != result
    }

    void "test dynamic-expiration-after-write-policy expires after set timeout"() {
        when:
        TimeService timeService = applicationContext.getBean(TimeService)
        long result = timeService.getTimeWithDynamicExpirationPolicy()

        then:
        timeService.getTimeWithDynamicExpirationPolicy() == result

        when:
        Thread.sleep(1500)

        then:
        timeService.getTimeWithDynamicExpirationPolicy() != result
    }


}
