from io.lettuce.core.codec import ByteArrayCodec, RedisCodec, StringCodec
from jakarta.inject import Named, Singleton
from micronaut.context.annotation import Factory, Requires


@Requires(property="spec.name", value="NamedCodecTest")
# tag::namedCodec[]
@Factory
class NamedCodecFactory:
# end::namedCodec[]

# tag::namedCodec2[]
    @Singleton
    @Named("foo")
    def foo_codec(self) -> RedisCodec[bytes, bytes]:
        return ByteArrayCodec.INSTANCE

    @Singleton
    @Named("bar")
    def bar_codec(self) -> RedisCodec[str, str]:
        return StringCodec.ASCII
# end::namedCodec2[]
