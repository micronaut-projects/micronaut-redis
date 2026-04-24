package io.micronaut.configuration.lettuce.docs.pubsub;

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener;
import io.micronaut.messaging.annotation.MessageBody;

@RedisListener
class BookCreatedListener {

    // tag::listener[]
    @MessageChannel("books.created")
    void receive(@MessageBody BookCreated event, @MessageChannel String channel) {
        System.out.println("Received " + event.getTitle() + " from " + channel);
    }
    // end::listener[]
}
