package io.micronaut.configuration.lettuce

import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = "spec.name", value = SPEC_NAME)
// tag::codecFactory[]
@Factory
class ByteArrayCodecFactory {

    static final String SPEC_NAME = "byte-array-codec-no-replacement"

    @Singleton
    RedisCodec<byte[], byte[]> redisCodec() {
        return ByteArrayCodec.INSTANCE
    }
}
// end::codecFactory[]
