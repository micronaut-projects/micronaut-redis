package io.micronaut.configuration.lettuce.docs

import groovy.transform.CompileStatic
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

// tag::namedCodec[]
@CompileStatic
@Factory
class NamedCodecFactory {
// end::namedCodec[]

// tag::namedCodec2[]
    @Singleton
    @Named("foo")
    RedisCodec<byte[], byte[]> fooCodec() {
        ByteArrayCodec.INSTANCE
    }

    @Singleton
    @Named("bar")
    RedisCodec<String, String> barCodec() {
        StringCodec.ASCII
    }
}
// end::namedCodec2[]
