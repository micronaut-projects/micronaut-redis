package io.micronaut.configuration.lettuce

import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

import java.lang.reflect.Proxy

@Requires(property = "spec.name", value = SPEC_NAME)
@Factory
class ClusterPubSubBeanReplacementFactory {

    static final String SPEC_NAME = "cluster-pubsub-bean"

    @Singleton
    @Replaces(RedisClusterClient)
    RedisClusterClient redisClusterClient() {
        return new RedisClusterClient() {
            @Override
            public <K, V> StatefulRedisClusterPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {
                return newClusterPubSubConnection()
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> StatefulRedisClusterPubSubConnection<K, V> newClusterPubSubConnection() {
        return (StatefulRedisClusterPubSubConnection<K, V>) Proxy.newProxyInstance(
            StatefulRedisClusterPubSubConnection.class.classLoader,
            [StatefulRedisClusterPubSubConnection] as Class<?>[],
            { Object proxy, java.lang.reflect.Method method, Object[] args ->
                switch (method.name) {
                    case "toString":
                        return "cluster-pubsub-test-connection"
                    case "hashCode":
                        return System.identityHashCode(proxy)
                    case "equals":
                        return proxy.is(args[0])
                    default:
                        return null
                }
            }
        )
    }
}
