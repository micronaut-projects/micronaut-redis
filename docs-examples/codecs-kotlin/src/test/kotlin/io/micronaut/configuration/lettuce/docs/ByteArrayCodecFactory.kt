package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = "spec.name", value = "ByteArrayCodecTest")
// tag::codecFactory[]
@Factory
class ByteArrayCodecFactory {

    @Singleton
    fun redisCodec(): RedisCodec<ByteArray, ByteArray> = ByteArrayCodec.INSTANCE
}
// end::codecFactory[]
