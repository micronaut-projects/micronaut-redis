package io.micronaut.configuration.lettuce

import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import spock.lang.Specification

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy

class DefaultRedisClusterClientFactoryCodecSpec extends Specification {

    void cleanup() {
        TestRedisClusterClient.reset()
    }

    void "test defined codec is passed to the cluster client"() {
        when:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.uris': ['redis://localhost:6379'],
                'spec.name': TestRedisClusterClientFactory.SPEC_NAME,
        )
        applicationContext.getBean(StatefulRedisClusterConnection)

        then:
        TestRedisClusterClient.lastCodec == ByteArrayCodec.INSTANCE

        cleanup:
        applicationContext.stop()
    }

    @Factory
    @Requires(property = "spec.name", value = SPEC_NAME)
    static class TestRedisClusterClientFactory {
        static final String SPEC_NAME = "cluster-byte-array-codec"

        @Singleton
        @Replaces(RedisCodec)
        RedisCodec<byte[], byte[]> redisCodec() {
            return ByteArrayCodec.INSTANCE
        }

        @Singleton
        @Primary
        @Replaces(RedisClusterClient)
        RedisClusterClient redisClusterClient() {
            return new TestRedisClusterClient()
        }
    }

    static final class TestRedisClusterClient extends RedisClusterClient {
        static RedisCodec<?, ?> lastCodec

        @Override
        <K, V> StatefulRedisClusterConnection<K, V> connect(RedisCodec<K, V> codec) {
            lastCodec = codec
            return (StatefulRedisClusterConnection<K, V>) Proxy.newProxyInstance(
                    StatefulRedisClusterConnection.class.classLoader,
                    [StatefulRedisClusterConnection] as Class[],
                    new DefaultInvocationHandler()
            )
        }

        static void reset() {
            lastCodec = null
        }
    }

    static final class DefaultInvocationHandler implements InvocationHandler {
        @Override
        Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            if (method.name == "equals") {
                return proxy.is(args[0])
            }
            if (method.name == "hashCode") {
                return System.identityHashCode(proxy)
            }
            if (method.name == "toString") {
                return "TestStatefulRedisClusterConnection"
            }
            Class<?> returnType = method.returnType
            if (returnType == Boolean.TYPE) {
                return false
            }
            if (returnType == Byte.TYPE) {
                return (byte) 0
            }
            if (returnType == Short.TYPE) {
                return (short) 0
            }
            if (returnType == Integer.TYPE) {
                return 0
            }
            if (returnType == Long.TYPE) {
                return 0L
            }
            if (returnType == Float.TYPE) {
                return 0F
            }
            if (returnType == Double.TYPE) {
                return 0D
            }
            if (returnType == Character.TYPE) {
                return (char) 0
            }
            return null
        }
    }
}
