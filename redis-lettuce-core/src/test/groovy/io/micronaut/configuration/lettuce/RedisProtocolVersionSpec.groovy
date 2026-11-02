package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.protocol.ProtocolVersion
import io.micronaut.context.ApplicationContext
import spock.lang.Specification

class RedisProtocolVersionSpec extends Specification {

    void "test the protocol version is applied to the client options"() {
        given:
        ApplicationContext context = ApplicationContext.run(
            "redis.uri": "redis://localhost:6379",
            "redis.protocol-version": "RESP2"
        )

        expect:
        context.getBean(RedisClient).options.protocolVersion == ProtocolVersion.RESP2

        cleanup:
        context.close()
    }

    void "test the client keeps the Lettuce default without a protocol version"() {
        given:
        ApplicationContext context = ApplicationContext.run("redis.uri": "redis://localhost:6379")

        expect:
        !context.getBean(AbstractRedisConfiguration).protocolVersion.present
        context.getBean(RedisClient).options.configuredProtocolVersion == null

        cleanup:
        context.close()
    }

    void "test the protocol version is applied to the cluster client options"() {
        given:
        ApplicationContext context = ApplicationContext.run(
            "redis.uris": ["redis://localhost:7000"],
            "redis.protocol-version": "RESP3"
        )

        expect:
        context.getBean(RedisClusterClient).options.protocolVersion == ProtocolVersion.RESP3

        cleanup:
        context.close()
    }
}
