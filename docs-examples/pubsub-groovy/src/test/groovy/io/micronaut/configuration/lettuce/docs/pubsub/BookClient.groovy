package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisPubSubClient
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces

@RedisPubSubClient
interface BookClient {

    // tag::client[]
    @MessageChannel("books.created")
    void publish(BookCreated event)
    // end::client[]

    // tag::clientProduces[]
    @MessageChannel("books.plain-text")
    @Produces(MediaType.TEXT_PLAIN)
    void publishPlain(String title)
    // end::clientProduces[]
}
