package io.micronaut.configuration.lettuce

import io.lettuce.core.ReadFrom
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection
import io.micronaut.context.ApplicationContext

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class RedisReplicaClientFactorySpec extends RedisReplicaSpec {

    void "test redis server config with replica uris"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.uri': primaryUri,
                'redis.replica-uris': redisReplicaUris,
        )
        StatefulRedisConnection connection = applicationContext.getBean(StatefulRedisConnection)

        then:
        // tag::commands[]
        RedisCommands<String, String> commands = connection.sync()
        commands.set("foo", "bar")
        commands.get("foo") == "bar"
        // end::commands[]
        connection instanceof StatefulRedisMasterReplicaConnection

        cleanup:
        applicationContext.stop()
    }

    void "test redis server config with read-from"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.uri': primaryUri,
                'redis.replica-uris': redisReplicaUris,
                'redis.read-from': "replicaPreferred"
        )
        StatefulRedisConnection connection = applicationContext.getBean(StatefulRedisConnection)

        then:
        // tag::commands[]
        RedisCommands<String, String> commands = connection.sync()
        commands.set("foo", "bar")
        commands.get("foo") == "bar"
        // end::commands[]
        connection instanceof StatefulRedisMasterReplicaConnection
        ((StatefulRedisMasterReplicaConnection) connection).readFrom == ReadFrom.REPLICA_PREFERRED

        cleanup:
        applicationContext.stop()
    }
}
