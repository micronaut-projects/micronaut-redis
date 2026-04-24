package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.core.annotation.Introspected

@Introspected
class BookCreated {
    String title
    String author
}
