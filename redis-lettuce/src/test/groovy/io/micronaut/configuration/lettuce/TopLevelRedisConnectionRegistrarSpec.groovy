package io.micronaut.configuration.lettuce

import io.lettuce.core.ReadFrom
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.micronaut.context.BeanContext
import io.micronaut.core.type.Argument
import io.micronaut.inject.BeanDefinition
import spock.lang.Specification

class TopLevelRedisConnectionRegistrarSpec extends Specification {

    void "top level standalone registrar registers connection beans for unqualified codecs"() {
        given:
        BeanContext beanContext = Mock()
        BeanDefinition<RedisCodec> codecDefinition = Mock()
        def registrar = new TopLevelRedisConnectionRegistrar(beanContext)

        when:
        registrar.registerBeans()

        then:
        1 * beanContext.getBeanDefinitions(RedisCodec) >> [codecDefinition]
        1 * codecDefinition.isPrimary() >> false
        1 * codecDefinition.getBeanName() >> Optional.empty()
        1 * codecDefinition.getTypeArguments(RedisCodec) >> [Argument.of(byte[].class), Argument.of(byte[].class)]
        2 * beanContext.registerBeanDefinition(_)
    }

    void "top level cluster registrar registers connection beans for unqualified codecs"() {
        given:
        BeanContext beanContext = Mock()
        BeanDefinition<RedisCodec> codecDefinition = Mock()
        def registrar = new TopLevelRedisClusterConnectionRegistrar(beanContext)

        when:
        registrar.registerBeans()

        then:
        1 * beanContext.getBeanDefinitions(RedisCodec) >> [codecDefinition]
        1 * codecDefinition.isPrimary() >> false
        1 * codecDefinition.getBeanName() >> Optional.empty()
        1 * codecDefinition.getTypeArguments(RedisCodec) >> [Argument.of(byte[].class), Argument.of(byte[].class)]
        2 * beanContext.registerBeanDefinition(_)
    }

    void "standalone registrar creates direct connection when no replicas are configured"() {
        given:
        BeanContext beanContext = Mock()
        RedisClient redisClient = Mock()
        AbstractRedisConfiguration config = Mock()
        RedisCodec codec = Mock()
        StatefulRedisConnection connection = Mock()
        def registrar = new TopLevelRedisConnectionRegistrar(beanContext)

        when:
        def result = invokePrivate(registrar, 'createConnection', Argument.of(byte[].class), Argument.of(byte[].class))

        then:
        1 * beanContext.getBean(RedisClient) >> redisClient
        1 * beanContext.getBean(AbstractRedisConfiguration) >> config
        1 * beanContext.getBean(_ as Argument) >> codec
        1 * config.getUri() >> Optional.empty()
        0 * config.getReplicaUris()
        1 * redisClient.connect(codec) >> connection
        result.is(connection)
    }

    void "standalone registrar creates pubsub connection for typed codec"() {
        given:
        BeanContext beanContext = Mock()
        RedisClient redisClient = Mock()
        RedisCodec codec = Mock()
        StatefulRedisPubSubConnection connection = Mock()
        def registrar = new TopLevelRedisConnectionRegistrar(beanContext)

        when:
        def result = invokePrivate(registrar, 'createPubSubConnection', Argument.of(byte[].class), Argument.of(byte[].class))

        then:
        1 * beanContext.getBean(RedisClient) >> redisClient
        1 * beanContext.getBean(_ as Argument) >> codec
        1 * redisClient.connectPubSub(codec) >> connection
        result.is(connection)
    }

    void "cluster registrar creates connection and applies read from"() {
        given:
        BeanContext beanContext = Mock()
        RedisClusterClient redisClient = Mock()
        AbstractRedisConfiguration config = Mock()
        RedisCodec codec = Mock()
        StatefulRedisClusterConnection connection = Mock()
        def registrar = new TopLevelRedisClusterConnectionRegistrar(beanContext)

        when:
        def result = invokePrivate(registrar, 'createConnection', Argument.of(byte[].class), Argument.of(byte[].class))

        then:
        1 * beanContext.getBean(RedisClusterClient) >> redisClient
        1 * beanContext.getBean(AbstractRedisConfiguration) >> config
        1 * beanContext.getBean(_ as Argument) >> codec
        1 * redisClient.connect(codec) >> connection
        1 * config.getReadFrom() >> Optional.of(ReadFrom.REPLICA_PREFERRED)
        1 * connection.setReadFrom(ReadFrom.REPLICA_PREFERRED)
        result.is(connection)
    }

    void "cluster registrar creates typed pubsub connection"() {
        given:
        BeanContext beanContext = Mock()
        RedisClusterClient redisClient = Mock()
        RedisCodec codec = Mock()
        StatefulRedisClusterPubSubConnection connection = Mock()
        def registrar = new TopLevelRedisClusterConnectionRegistrar(beanContext)

        when:
        def result = invokePrivate(registrar, 'createPubSubConnection', Argument.of(byte[].class), Argument.of(byte[].class))

        then:
        1 * beanContext.getBean(RedisClusterClient) >> redisClient
        1 * beanContext.getBean(_ as Argument) >> codec
        1 * redisClient.connectPubSub(codec) >> connection
        result.is(connection)
    }

    private static Object invokePrivate(Object target, String methodName, Argument<?> keyType, Argument<?> valueType) {
        def method = target.class.getDeclaredMethod(methodName, Argument, Argument)
        method.accessible = true
        return method.invoke(target, keyType, valueType)
    }
}
