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

    val created: BlockingQueue<String> = LinkedBlockingQueue()
    val plain: BlockingQueue<String> = LinkedBlockingQueue()

    @MessageChannel("books.created")
    fun created(@MessageBody event: BookCreated, @MessageChannel channel: String) {
        created.add("$channel:${event.title}:${event.author}")
    }

    @Consumes(MediaType.TEXT_PLAIN)
    @MessageChannel("books.plain-text")
    fun plain(@MessageBody body: String) {
        plain.add(body)
    }
}
