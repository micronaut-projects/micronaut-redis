package io.micronaut.configuration.lettuce

import io.lettuce.core.ReadFrom
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.micronaut.context.BeanLocator
import spock.lang.Specification

import java.net.URI
import java.util.Optional

class NamedRedisClientFactorySpec extends Specification {

    void "prefers named client resources when creating redis clients"() {
        given:
        BeanLocator beanLocator = Mock()
        RedisCodec<String, String> defaultCodec = Mock()
        ClientResources defaultResources = Mock()
        ClientResources namedResources = Mock()
        ClientResources.Builder builder = Mock()
        ClientResources builtResources = DefaultClientResources.create()
        NamedRedisClientFactory<String, String> factory = new NamedRedisClientFactory<>(beanLocator, defaultResources, defaultCodec)
        NamedRedisServersConfiguration configuration = new NamedRedisServersConfiguration("reports")
        configuration.setUri(URI.create("redis://localhost:6379"))
        List<ClientResourcesMutator> mutators = [Mock(ClientResourcesMutator)]
        RedisClient result = null

        when:
        result = factory.redisClient(configuration, mutators)

        then:
        result != null
        1 * beanLocator.findBean(ClientResources, _) >> Optional.of(namedResources)
        1 * namedResources.mutate() >> builder
        1 * mutators[0].mutate(builder, configuration)
        1 * builder.build() >> builtResources
        0 * defaultResources._

        cleanup:
        result?.shutdown()
        builtResources.shutdown()
    }

    void "uses the default codec for standalone connections when no named codec exists"() {
        given:
        BeanLocator beanLocator = Mock()
        RedisCodec<String, String> defaultCodec = Mock()
        RedisClient redisClient = Mock()
        StatefulRedisConnection<String, String> connection = Mock()
        NamedRedisClientFactory<String, String> factory = new NamedRedisClientFactory<>(beanLocator, null, defaultCodec)
        NamedRedisServersConfiguration configuration = new NamedRedisServersConfiguration("reports")
        configuration.setUri(URI.create("redis://localhost:6379"))

        when:
        StatefulRedisConnection<String, String> result = factory.redisConnection(configuration)

        then:
        result.is(connection)
        1 * beanLocator.findBean(RedisCodec, _) >> Optional.empty()
        1 * beanLocator.getBean(RedisClient, _) >> redisClient
        1 * redisClient.connect(defaultCodec) >> connection
        0 * _
    }

    void "uses the default codec for pubsub connections when no named codec exists"() {
        given:
        BeanLocator beanLocator = Mock()
        RedisCodec<String, String> defaultCodec = Mock()
        RedisClient redisClient = Mock()
        StatefulRedisPubSubConnection<String, String> connection = Mock()
        NamedRedisClientFactory<String, String> factory = new NamedRedisClientFactory<>(beanLocator, null, defaultCodec)
        NamedRedisServersConfiguration configuration = new NamedRedisServersConfiguration("reports")

        when:
        StatefulRedisPubSubConnection<String, String> result = factory.redisPubSubConnection(configuration)

        then:
        result.is(connection)
        1 * beanLocator.findBean(RedisCodec, _) >> Optional.empty()
        1 * beanLocator.getBean(RedisClient, _) >> redisClient
        1 * redisClient.connectPubSub(defaultCodec) >> connection
        0 * _
    }
}
