package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.SslVerifyMode
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
        configuration.setClientName("default-client")
        configuration.setSsl(true)
        configuration.setStartTls(true)
        configuration.setVerifyPeer(false)

        when:
        RedisURI mergedUri = configuration.getUri().orElseThrow()
        RedisClient client = new DefaultRedisClientFactory<String, String>(StringCodec.UTF8).redisClient(configuration)

        then:
        mergedUri.timeout == Duration.ofSeconds(1)
        mergedUri.database == 4
        mergedUri.clientName == "default-client"
        mergedUri.ssl
        mergedUri.startTls
        !mergedUri.verifyPeer
        client.@redisURI.timeout == Duration.ofSeconds(1)
        client.@redisURI.database == 4
        client.@redisURI.clientName == "default-client"
        client.@redisURI.ssl
        client.@redisURI.startTls
        !client.@redisURI.verifyPeer

        cleanup:
        client.shutdown()
    }

    void "test named uri collections apply separately bound RedisURI settings"() {
        given:
        NamedRedisServersConfiguration configuration = new NamedRedisServersConfiguration("reports")
        configuration.setUris(URI.create("redis://primary:6379"), URI.create("redis://secondary:6379"))
        configuration.setReplicaUris(URI.create("redis://replica:6379"))
        configuration.setTimeout(Duration.ofSeconds(5))
        configuration.setDatabase(7)
        configuration.setClientName("named-client")
        configuration.setSsl(true)
        configuration.setStartTls(true)
        configuration.setVerifyPeer(SslVerifyMode.CA)

        when:
        List<RedisURI> clusterUris = configuration.getUris()
        List<RedisURI> replicaUris = configuration.getReplicaUris()

        then:
        clusterUris*.timeout == [Duration.ofSeconds(5), Duration.ofSeconds(5)]
        clusterUris*.database == [7, 7]
        clusterUris*.clientName == ["named-client", "named-client"]
        clusterUris*.ssl == [true, true]
        clusterUris*.startTls == [true, true]
        clusterUris*.verifyMode == [SslVerifyMode.CA, SslVerifyMode.CA]
        replicaUris*.timeout == [Duration.ofSeconds(5)]
        replicaUris*.database == [7]
        replicaUris*.clientName == ["named-client"]
        replicaUris*.ssl == [true]
        replicaUris*.startTls == [true]
        replicaUris*.verifyMode == [SslVerifyMode.CA]
    }
}
