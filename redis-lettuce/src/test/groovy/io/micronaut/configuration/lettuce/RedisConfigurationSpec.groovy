package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException

import spock.lang.AutoCleanup
import spock.lang.Specification

import java.time.Duration


class RedisConfigurationSpec extends Specification {
    @AutoCleanup ApplicationContext applicationContext

    void "test AbstractRedisConfiguration not available when disabled"() {
        given:
        applicationContext = ApplicationContext.run(["redis.enabled": false])

        when:
        applicationContext.getBean(AbstractRedisConfiguration)

        then:
        thrown(NoSuchBeanException)
    }

    def "test StatefulRedisConnection not available when disabled"() {
        given:
        applicationContext = ApplicationContext.run(['redis.enabled': false])

        when:
        applicationContext.getBean(StatefulRedisConnection)

        then:
        thrown(NoSuchBeanException)
    }

    def "test RedisClient not available when disabled"() {
        given:
        applicationContext = ApplicationContext.run(['redis.enabled': false])

        when:
        applicationContext.getBean(RedisClient)

        then:
        thrown(NoSuchBeanException)
    }

    void "test uri configuration applies separately bound RedisURI settings"() {
        given:
        DefaultRedisConfiguration configuration = new DefaultRedisConfiguration()
        configuration.setUri(URI.create("redis://localhost:6379"))
        configuration.setTimeout(Duration.ofSeconds(1))
        configuration.setDatabase(4)
        configuration.setSsl(true)

        when:
        RedisURI mergedUri = configuration.getUri().orElseThrow()
        RedisClient client = new DefaultRedisClientFactory<String, String>(StringCodec.UTF8).redisClient(configuration)

        then:
        mergedUri.timeout == Duration.ofSeconds(1)
        mergedUri.database == 4
        mergedUri.ssl
        client != null

        cleanup:
        client.shutdown()
    }
}
