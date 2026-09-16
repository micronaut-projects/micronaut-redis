from jakarta.inject import Singleton
from micronaut.context.annotation import Factory, Requires

try:
    from io.lettuce.core.codec import ByteArrayCodec, RedisCodec
except ImportError:  # TODO(python): packages under `io.` other than `io.micronaut` cannot be imported at runtime
    from lettuce.core.codec import ByteArrayCodec, RedisCodec


@Requires(property="spec.name", value="ByteArrayCodecTest")
# tag::codecFactory[]
@Factory
class ByteArrayCodecFactory:

    @Singleton
    def redis_codec(self) -> RedisCodec[bytes, bytes]:
        return ByteArrayCodec.INSTANCE
# end::codecFactory[]
