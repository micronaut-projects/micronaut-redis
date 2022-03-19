package io.micronaut.configuration.lettuce.pubsub;

import io.micronaut.configuration.lettuce.RedisSetting;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.runtime.ApplicationConfiguration;

import java.util.Optional;

@EachProperty(RedisSetting.REDIS_CACHES)
@ConfigurationProperties(RedisSetting.REDIS_PUBSUB)
public class RedisPubSubConfiguration {

    protected String server;
    protected Class<ObjectSerializer> channelSerializer;
    protected Class<ObjectSerializer> valueSerializer;

    public RedisPubSubConfiguration(ApplicationConfiguration applicationConfiguration) {}

    public Optional<String> getServer() {
        return Optional.ofNullable(server);
    }

    public Optional<Class<ObjectSerializer>> getChannelSerializer() {
        return Optional.ofNullable(channelSerializer);
    }

    public Optional<Class<ObjectSerializer>> getValueSerializer() {
        return Optional.ofNullable(valueSerializer);
    }

}
