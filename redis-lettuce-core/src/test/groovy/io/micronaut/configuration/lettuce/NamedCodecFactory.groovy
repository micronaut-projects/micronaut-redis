package io.micronaut.configuration.lettuce

import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Requires(property = "spec.name", value = SPEC_NAME)
//tag::namedCodec[]
@Factory
class NamedCodecFactory {
//end::namedCodec[]

   static final String SPEC_NAME = "named-codecs"
//tag::namedCodec2[]

    @Singleton
    @Named("foo")
    RedisCodec<byte[], byte[]> fooCodec() {
        return ByteArrayCodec.INSTANCE
    }

    @Singleton
    @Named("bar")
    RedisCodec<String, String> barCodec() {
        return StringCodec.ASCII
    }
}
//end::namedCodec2[]
