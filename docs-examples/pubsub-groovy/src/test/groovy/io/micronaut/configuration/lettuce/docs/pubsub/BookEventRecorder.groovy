package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Consumes
import io.micronaut.messaging.annotation.MessageBody

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

/**
 * Records the messages published by the documented publisher and client, next to the documented listeners.
 */
@RedisListener
class BookEventRecorder {

    final BlockingQueue<String> created = new LinkedBlockingQueue<>()
    final BlockingQueue<String> plain = new LinkedBlockingQueue<>()

    @MessageChannel("books.created")
    void created(@MessageBody BookCreated event, @MessageChannel String channel) {
        created.add("${channel}:${event.title}:${event.author}".toString())
    }

    @Consumes(MediaType.TEXT_PLAIN)
    @MessageChannel("books.plain-text")
    void plain(@MessageBody String body) {
        plain.add(body)
    }
}
