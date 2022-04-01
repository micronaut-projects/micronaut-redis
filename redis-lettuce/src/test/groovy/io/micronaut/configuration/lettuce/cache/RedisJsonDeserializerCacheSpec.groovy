package io.micronaut.configuration.lettuce.cache

import io.micronaut.context.ApplicationContext

/**
 * @author Denis Stepanov
 */
class RedisJsonDeserializerCacheSpec extends RedisCacheSpec {

    ApplicationContext createApplicationContext() {
        ApplicationContext.run(
                'redis.port': redisPort,
                'redis.caches.test.enabled': 'true',
                'redis.caches.test.valueSerializer': 'io.micronaut.jackson.serialize.JacksonObjectSerializer'
        )
    }
}
