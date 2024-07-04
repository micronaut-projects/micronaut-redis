package io.micronaut.configuration.lettuce

import io.lettuce.core.ReadFrom
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.ConfigurationException

class DefaultRedisClusterClientFactorySpec extends RedisClusterSpec {

    void "test redis cluster connection"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uris': redisClusterUris,
        ])
        RedisClusterClient client = applicationContext.getBean(RedisClusterClient)

        when:
        fixPartitions(client)
        StatefulRedisClusterConnection<String, String> connection = applicationContext.getBean(StatefulRedisClusterConnection<String, String>)
        def command = connection.sync()
        command.set("foo", "bar")

        then:
        command.get("foo") == "bar"

        cleanup:
        applicationContext.stop()
    }

    void "test redis cluster connection read-from"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uris': redisClusterUris,
                'redis.read-from': "replicaPreferred"
        ])
        RedisClusterClient client = applicationContext.getBean(RedisClusterClient)

        when:
        fixPartitions(client)
        StatefulRedisClusterConnection<String, String> connection = applicationContext.getBean(StatefulRedisClusterConnection<String, String>)
        def command = connection.sync()
        command.set("foo-readFrom", "bar-readFrom")

        then:
        command.get("foo-readFrom") == "bar-readFrom"
        connection.getReadFrom() == ReadFrom.REPLICA_PREFERRED

        cleanup:
        applicationContext.stop()
    }

    void "test redis cluster pubsub connection"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uris': redisClusterUris,
        ])
        RedisClusterClient client = applicationContext.getBean(RedisClusterClient)

        when:
        fixPartitions(client)
        StatefulRedisPubSubConnection<String, String> connection = applicationContext.getBean(StatefulRedisPubSubConnection<String, String>)
        def command = connection.sync()
        command.set("lorem", "ipsum")

        then:
        command.get("lorem") == "ipsum"

        cleanup:
        applicationContext.stop()
    }

    void "test redis cluster with metrics"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uris': redisClusterUris,
        ])
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)
        RedisClusterClient client = applicationContext.getBean(RedisClusterClient)

        when:
        client.refreshPartitions()

        then:
        meterRegistry.getMeters().findAll {it.getId().getName().startsWith("lettuce")}.size() > 0

        cleanup:
        applicationContext.stop()
    }

    void "test redis cluster without metrics"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uris': redisClusterUris,
        ])
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)
        DefaultRedisClusterClientFactory factory = applicationContext.getBean(DefaultRedisClusterClientFactory)
        AbstractRedisConfiguration config = applicationContext.getBean(AbstractRedisConfiguration)
        RedisClusterClient client = factory.redisClient(config, null)

        when:
        client.refreshPartitions()

        then:
        meterRegistry.getMeters().findAll {it.getId().getName().startsWith("lettuce")}.empty

        cleanup:
        applicationContext.stop()
    }

    void "test redis cluster - wrong config"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(['redis.uris': ''])
        DefaultRedisClusterClientFactory factory = applicationContext.getBean(DefaultRedisClusterClientFactory)
        AbstractRedisConfiguration config = new NamedRedisServersConfiguration("wrong config")

        when:
        factory.redisClient(config, null)

        then:
        thrown(ConfigurationException)
    }
}
