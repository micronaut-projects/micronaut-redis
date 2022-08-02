package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import spock.lang.Specification

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class RedisClientFactorySpec extends Specification implements RedisContainerTrait {

    private static final String MAX_HEAP_SETTING = "maxmemory 256M"

    void "test redis server config by port"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run('redis.port': redisPort)
        StatefulRedisConnection connection = applicationContext.getBean(StatefulRedisConnection)

        then:
        // tag::commands[]
        RedisCommands<String, String> commands = connection.sync()
        commands.set("foo", "bar")
        commands.get("foo") == "bar"
        // end::commands[]

        cleanup:
        applicationContext.stop()
    }

    void "test redis server config by URI"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run('redis.uri': getRedisPort("redis://localhost"))
        StatefulRedisConnection client = applicationContext.getBean(StatefulRedisConnection)
        RedisCommands<?,?> command = client.sync()
        then:
        command.set("foo", "bar")
        command.get("foo") == "bar"

        cleanup:
        applicationContext.stop()
    }

    void "test multi redis server config by URI"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(['redis.servers.foo.uri': getRedisPort("redis://localhost"),
                                                                        'redis.servers.foo.client-name': "foo-client-name",
                                                                        'redis.servers.bar.uri': getRedisPort("redis://localhost")])
        when:
        RedisClient clientFoo = applicationContext.getBean(RedisClient, Qualifiers.byName("foo"))
        RedisURI innerRedisURI = clientFoo.@redisURI
        RedisCommands<?,?> commandFoo = clientFoo.connect().sync()

        then:
        innerRedisURI.port == redisPort
        innerRedisURI.clientName == "foo-client-name"
        commandFoo.info().contains("tcp_port:$REDIS_PORT")
        commandFoo.set("foo", "bar")
        commandFoo.get("foo") == "bar"

        when:
        RedisClient clientBar = applicationContext.getBean(RedisClient, Qualifiers.byName("bar"))
        RedisURI innerBarRedisURI = clientBar.@redisURI
        RedisCommands<?,?> commandBar = clientBar.connect().sync()
        then:
        commandBar.info().contains("tcp_port:$REDIS_PORT")
        !innerBarRedisURI.clientName

        cleanup:
        applicationContext.stop()
    }

    void "test redis thread pool settings"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uri': getRedisPort("redis://localhost"),
                'redis.io-thread-pool-size':10,
                'redis.computation-thread-pool-size':20,
        ])
        when:
        RedisClient client = applicationContext.getBean(RedisClient)

        then:
        client.getResources().computationThreadPoolSize() == 20
        client.getResources().ioThreadPoolSize() == 10

        cleanup:
        applicationContext.stop()
    }

    void "test redis client name settings"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uri': getRedisPort("redis://localhost"),
                'redis.client-name':"test-name"
        ])
        when:
        RedisClient client = applicationContext.getBean(RedisClient)
        RedisURI innerRedisURI = client.@redisURI

        then:
        innerRedisURI.clientName == "test-name"

        cleanup:
        applicationContext.stop()
    }

    void "test redis metrics settings"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uri': getRedisPort("redis://localhost"),
        ])

        when:
        StatefulRedisConnection client = applicationContext.getBean(StatefulRedisConnection)
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)
        def command = client.sync()
        command.set("foo", "bar")

        then:
        meterRegistry.getMeters().findAll {it.getId().getName().startsWith("lettuce")}.size() > 0

        cleanup:
        applicationContext.stop()
    }
}
