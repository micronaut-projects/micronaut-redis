package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Requires(property = "spec.name", value = "NamedCodecTest")
// tag::namedCodec[]
@Factory
class NamedCodecFactory {
// end::namedCodec[]

// tag::namedCodec2[]
    @Singleton
    @Named("foo")
    fun fooCodec(): RedisCodec<ByteArray, ByteArray> = ByteArrayCodec.INSTANCE

    @Singleton
    @Named("bar")
    fun barCodec(): RedisCodec<String, String> = StringCodec.ASCII
}
// end::namedCodec2[]
