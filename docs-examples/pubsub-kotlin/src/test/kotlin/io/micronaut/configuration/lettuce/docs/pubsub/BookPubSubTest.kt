package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.RedisPubSubPublisher
import io.micronaut.redis.testcontainers.Redis
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.TimeUnit

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BookPubSubTest : TestPropertyProvider {

    @Inject
    lateinit var publisher: RedisPubSubPublisher

    @Inject
    lateinit var bookPublisher: BookPublisher

    @Inject
    lateinit var bookClient: BookClient

    @Inject
    lateinit var recorder: BookEventRecorder

    override fun getProperties(): Map<String, String> = Redis.getProperties()

    @Test
    fun testPublisherAndClientPublishJsonEvents() {
        bookPublisher.publish(BookCreated("Dune", "Frank Herbert"))
        bookClient.publish(BookCreated("Neuromancer", "William Gibson"))

        assertEquals(
            setOf("books.created:Dune:Frank Herbert", "books.created:Neuromancer:William Gibson"),
            setOf(recorder.created.poll(10, TimeUnit.SECONDS), recorder.created.poll(10, TimeUnit.SECONDS))
        )
    }

    @Test
    fun testClientPublishesPlainText() {
        bookClient.publishPlain("Foundation")

        assertEquals("Foundation", recorder.plain.poll(10, TimeUnit.SECONDS))
    }

    @Test
    fun testAuditListenerIsSubscribed() {
        // the returned subscriber count proves that the failing audit listener is subscribed
        assertTrue(publisher.publish("books.audit", BookCreated("Dune", "Frank Herbert")) >= 1)
    }
}
