package io.micronaut.configuration.lettuce.docs

import groovy.transform.CompileStatic
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

// tag::codecFactory[]
@CompileStatic
@Factory
class ByteArrayCodecFactory {

    @Singleton
    RedisCodec<byte[], byte[]> redisCodec() {
        ByteArrayCodec.INSTANCE
    }
}
// end::codecFactory[]
