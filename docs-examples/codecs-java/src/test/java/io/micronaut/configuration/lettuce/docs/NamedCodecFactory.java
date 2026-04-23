package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

// tag::namedCodec[]
@Factory
public class NamedCodecFactory {
// end::namedCodec[]

// tag::namedCodec2[]
    @Singleton
    @Named("foo")
    RedisCodec<byte[], byte[]> fooCodec() {
        return ByteArrayCodec.INSTANCE;
    }

    @Singleton
    @Named("bar")
    RedisCodec<String, String> barCodec() {
        return StringCodec.ASCII;
    }
}
// end::namedCodec2[]
