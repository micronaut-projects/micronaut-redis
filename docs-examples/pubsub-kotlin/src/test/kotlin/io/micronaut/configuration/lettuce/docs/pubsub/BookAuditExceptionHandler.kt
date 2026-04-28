package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerException
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler
import jakarta.inject.Singleton

@Singleton
class BookAuditExceptionHandler : RedisListenerExceptionHandler {

    // tag::handler[]
    override fun handle(exception: RedisListenerException) {
        System.err.println("Redis listener failure on ${exception.messageChannel}: ${exception.message}")
    }
    // end::handler[]
}
