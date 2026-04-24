package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.RedisPubSubPublisher
import jakarta.inject.Singleton

@Singleton
class BookPublisher(private val publisher: RedisPubSubPublisher) {

    // tag::publisher[]
    fun publish(event: BookCreated) {
        publisher.publish("books.created", event)
    }
    // end::publisher[]
}
