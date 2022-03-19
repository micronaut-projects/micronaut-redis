package io.micronaut.configuration.lettuce.pubsub.processor;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.configuration.lettuce.pubsub.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.configuration.lettuce.pubsub.RedisPubSubConfiguration;
import io.micronaut.configuration.lettuce.pubsub.RedisSubscriber;
import io.micronaut.context.BeanLocator;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.serialize.JdkSerializer;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

@Singleton
public class RedisPubSub {

    private final ObjectSerializer channelSerializer;
    private final ObjectSerializer valueSerializer;
    private final RedisPubSubCommands<byte[], byte[]> syncCommands;
    private final StatefulRedisPubSubConnection<byte[], byte[]> connection;
    private final MessageRouter messageRouter = new MessageRouter();
    private final ExecutorService executorService;
    private RedisSubscriber subscriber;

    public RedisPubSub(
            @Named(TaskExecutors.MESSAGE_CONSUMER) ExecutorService executorService,
            ConversionService<?> conversionService,
            BeanLocator beanLocator) {

        this.executorService = executorService;

        RedisPubSubConfiguration configuration = new RedisPubSubConfiguration(null);
        this.valueSerializer = configuration.getValueSerializer()
                .flatMap(beanLocator::findOrInstantiateBean)
                .orElseGet(() -> new JdkSerializer(conversionService));

        this.channelSerializer = configuration.getChannelSerializer()
                .flatMap(beanLocator::findOrInstantiateBean)
                .orElseGet(() -> new JdkSerializer(conversionService));

        this.connection = RedisConnectionUtil.openBytesRedisPubSubConnection(beanLocator,
                configuration.getServer(), "No Redis server configured.");

        this.syncCommands = connection.sync();

        this.connection.addListener(messageRouter);
    }

    void subscribe(Set<MessageChannel> channels, RedisSubscriber subscriber) {
        syncCommands.subscribe(channels.stream().map(this::serializeChannel).toArray(byte[][]::new));
        this.messageRouter.addSubscriber(channels, subscriber);
        this.subscriber = subscriber;
    }

    public void publish(MessageChannel channel, String message) {
        syncCommands.publish(serializeChannel(channel), serializeValue(message));
    }

    private byte[] serializeChannel(MessageChannel channel) {
        // FIXME -- proper serialization
        return channel.getValue().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] serializeValue(Object value) {
        return valueSerializer.serialize(value).orElse(null);
    }

    private class MessageRouter implements RedisPubSubListener<byte[], byte[]> {

        final Map<MessageChannel, Set<RedisSubscriber>> subscribers = new HashMap<>();

        void addSubscriber(Set<MessageChannel> channels, RedisSubscriber subscriber) {
            // TODO
            channels.forEach(channel -> subscribers.put(channel, Collections.singleton(subscriber)));
        }

        @Override
        public void message(byte[] channelArg, byte[] message) {
            // FIXME -- Proper conversion
            MessageChannel channel = MessageChannel.individual(new String(channelArg));
            subscribers.get(channel).forEach(subscriber -> executorService.submit(() -> {
                Publisher<RedisMessage> publisher = Publishers.just(new RedisMessage(message, channel));
                publisher.subscribe(subscriber);
            }));
        }

        @Override
        public void message(byte[] pattern, byte[] channel, byte[] message) {
            // TODO
        }

        @Override
        public void subscribed(byte[] channel, long count) {
            // TODO
        }

        @Override
        public void psubscribed(byte[] pattern, long count) {
            // TODO
        }

        @Override
        public void unsubscribed(byte[] channel, long count) {
            // TODO
        }

        @Override
        public void punsubscribed(byte[] pattern, long count) {
            // TODO
        }

    }

}
