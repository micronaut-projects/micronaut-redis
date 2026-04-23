package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

// tag::codecFactory[]
@Factory
class ByteArrayCodecFactory {

    @Singleton
    fun redisCodec(): RedisCodec<ByteArray, ByteArray> = ByteArrayCodec.INSTANCE
}
// end::codecFactory[]
