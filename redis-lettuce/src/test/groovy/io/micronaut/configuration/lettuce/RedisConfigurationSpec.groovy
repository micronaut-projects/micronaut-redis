package io.micronaut.configuration.lettuce

import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException

import spock.lang.AutoCleanup
import spock.lang.Specification


class RedisConfigurationSpec extends Specification {
    @AutoCleanup ApplicationContext applicationContext

    void "test disabled"() {
        given:
        applicationContext = ApplicationContext.run(["redis.enabled": false])

        when:
        applicationContext.getBean(AbstractRedisConfiguration)

        then:
        thrown(NoSuchBeanException)
    }
}
