package io.micronaut.configuration.lettuce.docs.pubsub;

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.messaging.annotation.MessageBody;

@RedisListener
class PlainTextBookListener {

    // tag::listener[]
    @Consumes(MediaType.TEXT_PLAIN)
    @MessageChannel("books.plain-text")
    void receive(@MessageBody String title) {
        System.out.println("Received plain text title " + title);
    }
    // end::listener[]
}
