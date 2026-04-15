package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

// tag::codecFactory[]
@Factory
public class ByteArrayCodecFactory {

    @Singleton
    RedisCodec<byte[], byte[]> redisCodec() {
        return ByteArrayCodec.INSTANCE;
    }
}
// end::codecFactory[]
