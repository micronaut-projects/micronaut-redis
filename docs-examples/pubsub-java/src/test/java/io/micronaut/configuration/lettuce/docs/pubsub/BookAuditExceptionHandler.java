package io.micronaut.configuration.lettuce.docs.pubsub;

import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerException;
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler;
import jakarta.inject.Singleton;

@Singleton
class BookAuditExceptionHandler implements RedisListenerExceptionHandler {

    // tag::handler[]
    @Override
    public void handle(RedisListenerException exception) {
        System.err.println("Redis listener failure on " + exception.getMessageChannel() + ": " + exception.getMessage());
    }
    // end::handler[]
}
