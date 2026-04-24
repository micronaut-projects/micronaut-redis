package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener
import io.micronaut.messaging.annotation.MessageBody

@RedisListener
class BookCreatedListener {

    // tag::listener[]
    @MessageChannel("books.created")
    void receive(@MessageBody BookCreated event, @MessageChannel String channel) {
        println "Received ${event.title} from ${channel}"
    }
    // end::listener[]
}
