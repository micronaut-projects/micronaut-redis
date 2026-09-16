package io.micronaut.configuration.lettuce.docs

import groovy.transform.CompileStatic
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = "spec.name", value = "ByteArrayCodecTest")
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
