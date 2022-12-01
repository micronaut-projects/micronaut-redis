package io.micronaut.configuration.lettuce.pubsub.subscriber;

import io.micronaut.configuration.lettuce.pubsub.annotations.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.annotations.RedisListener;
import io.micronaut.messaging.annotation.MessageBody;

import java.util.ArrayList;
import java.util.List;

@RedisListener
class MessageChannelSubscriber {

    public List<byte[]> log = new ArrayList<>();

    @MessageChannel("individual_channel")
    public void handle(@MessageBody byte[] message) {
        log.add(message);
    }

}
