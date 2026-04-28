package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.core.annotation.Introspected

@Introspected
data class BookCreated(
    val title: String,
    val author: String
)
