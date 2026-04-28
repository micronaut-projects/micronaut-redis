package io.micronaut.configuration.lettuce.docs.pubsub;

import io.micronaut.configuration.lettuce.pubsub.RedisPubSubPublisher;
import jakarta.inject.Singleton;

@Singleton
class BookPublisher {
    private final RedisPubSubPublisher publisher;

    BookPublisher(RedisPubSubPublisher publisher) {
        this.publisher = publisher;
    }

    // tag::publisher[]
    void publish(BookCreated event) {
        publisher.publish("books.created", event);
    }
    // end::publisher[]
}
