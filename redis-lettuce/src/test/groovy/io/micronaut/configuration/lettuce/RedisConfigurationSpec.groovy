package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.RedisClient
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException

import spock.lang.AutoCleanup
import spock.lang.Specification


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
}
