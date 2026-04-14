package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.RedisClient
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.inject.qualifiers.Qualifiers
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

    void "test redis metrics configuration binds command latency recorder settings"() {
        given:
        applicationContext = ApplicationContext.run([
                'redis.uri': 'redis://localhost:6379',
                'redis.metrics.command-latency-recorder.histogram': false,
                'redis.metrics.command-latency-recorder.target-percentiles': [0.25d, 0.75d],
                'redis.servers.foo.uri': 'redis://localhost:6379',
                'redis.servers.foo.metrics.command-latency-recorder.enabled': false
        ])

        when:
        def defaultConfig = applicationContext.getBean(AbstractRedisConfiguration.RedisCommandLatencyRecorderConfiguration)
        def namedConfig = applicationContext.getBean(AbstractRedisConfiguration.RedisCommandLatencyRecorderConfiguration, Qualifiers.byName("foo"))

        then:
        !defaultConfig.isHistogram()
        defaultConfig.getTargetPercentiles().toList() == [0.25d, 0.75d]
        !namedConfig.isEnabled()
        namedConfig.isHistogram()
    }
}
