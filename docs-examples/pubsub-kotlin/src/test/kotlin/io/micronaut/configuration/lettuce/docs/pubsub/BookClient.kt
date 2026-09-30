package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisPubSubClient
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces

@RedisPubSubClient
interface BookClient {

    // tag::client[]
    @MessageChannel("books.created")
    fun publish(event: BookCreated)
    // end::client[]

    // tag::clientProduces[]
    @MessageChannel("books.plain-text")
    @Produces(MediaType.TEXT_PLAIN)
    fun publishPlain(title: String)
    // end::clientProduces[]
}
