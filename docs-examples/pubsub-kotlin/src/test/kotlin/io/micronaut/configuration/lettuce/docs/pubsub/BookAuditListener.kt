package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener
import io.micronaut.messaging.annotation.MessageBody

@RedisListener
class BookAuditListener {

    // tag::listener[]
    @MessageChannel(value = "books.audit", exceptionHandler = BookAuditExceptionHandler::class)
    fun receive(@MessageBody event: BookCreated) {
        throw IllegalStateException("Could not audit ${event.title}")
    }
    // end::listener[]
}
