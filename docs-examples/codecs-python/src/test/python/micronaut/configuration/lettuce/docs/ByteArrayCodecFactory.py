from io.lettuce.core.codec import ByteArrayCodec, RedisCodec
from jakarta.inject import Singleton
from micronaut.context.annotation import Factory, Requires


@Requires(property="spec.name", value="ByteArrayCodecTest")
# tag::codecFactory[]
@Factory
class ByteArrayCodecFactory:

    @Singleton
    def redis_codec(self) -> RedisCodec[bytes, bytes]:
        return ByteArrayCodec.INSTANCE
# end::codecFactory[]
