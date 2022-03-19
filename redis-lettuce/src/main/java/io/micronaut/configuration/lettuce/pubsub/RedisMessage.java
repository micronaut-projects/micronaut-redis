package io.micronaut.configuration.lettuce.pubsub;

public class RedisMessage {

    private final Object body;
    private final MessageChannel channel;

    public RedisMessage(Object body, MessageChannel channel) {
        this.body = body;
        this.channel = channel;
    }

    public Object getBody() {
        return body;
    }

    public MessageChannel getChannel() {
        return channel;
    }

}
