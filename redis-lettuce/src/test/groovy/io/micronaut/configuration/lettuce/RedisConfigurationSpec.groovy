package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisCredentialsProvider
import io.lettuce.core.RedisURI
import io.lettuce.core.SslVerifyMode
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.inject.qualifiers.Qualifiers
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.net.URI
import java.time.Duration
import java.util.List


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

    void "test uri authentication configuration is applied when bound from properties"() {
        given:
        applicationContext = ApplicationContext.run([
                'redis.uri'           : 'redis://localhost:6379',
                'redis.authentication': 's3cret'
        ])

        when:
        DefaultRedisConfiguration configuration = applicationContext.getBean(DefaultRedisConfiguration)
        RedisClient client = applicationContext.getBean(RedisClient)

        then:
        passwordOf(configuration) == 's3cret'
        passwordOf(configuration.getUri().orElseThrow()) == 's3cret'
        passwordOf(client.@redisURI) == 's3cret'

        cleanup:
        client.shutdown()
    }

    void "test uri configuration applies separately bound RedisURI settings"() {
        given:
        DefaultRedisConfiguration configuration = new DefaultRedisConfiguration()
        configuration.setUri(URI.create("redis://localhost:6379"))
        configuration.setAuthentication("s3cret")
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
        passwordOf(mergedUri) == 's3cret'
        mergedUri.timeout == Duration.ofSeconds(1)
        mergedUri.database == 4
        mergedUri.clientName == "default-client"
        mergedUri.ssl
        mergedUri.startTls
        !mergedUri.verifyPeer
        passwordOf(client.@redisURI) == 's3cret'
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
        configuration.setAuthentication("named-secret")
        configuration.setSsl(true)
        configuration.setStartTls(true)
        configuration.setVerifyPeer(SslVerifyMode.CA)

        when:
        List<RedisURI> clusterUris = configuration.getUris()
        List<RedisURI> replicaUris = configuration.getReplicaUris()

        then:
        clusterUris.collect(this::passwordOf) == ['named-secret', 'named-secret']
        clusterUris*.timeout == [Duration.ofSeconds(5), Duration.ofSeconds(5)]
        clusterUris*.database == [7, 7]
        clusterUris*.clientName == ["named-client", "named-client"]
        clusterUris*.ssl == [true, true]
        clusterUris*.startTls == [true, true]
        clusterUris*.verifyMode == [SslVerifyMode.CA, SslVerifyMode.CA]
        replicaUris.collect(this::passwordOf) == ['named-secret']
        replicaUris*.timeout == [Duration.ofSeconds(5)]
        replicaUris*.database == [7]
        replicaUris*.clientName == ["named-client"]
        replicaUris*.ssl == [true]
        replicaUris*.startTls == [true]
        replicaUris*.verifyMode == [SslVerifyMode.CA]
    }

    private static String passwordOf(AbstractRedisConfiguration configuration) {
        return passwordOf((RedisURI) configuration)
    }

    private static String passwordOf(RedisURI redisURI) {
        RedisCredentialsProvider credentialsProvider = redisURI.getCredentialsProvider()
        def credentials = credentialsProvider.resolveCredentials().block()
        return credentials?.hasPassword() ? new String(credentials.password) : null
    }
}
