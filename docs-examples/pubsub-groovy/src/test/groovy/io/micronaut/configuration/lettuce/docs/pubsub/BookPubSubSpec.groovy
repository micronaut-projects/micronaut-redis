package io.micronaut.configuration.lettuce.docs.pubsub

import io.micronaut.configuration.lettuce.pubsub.RedisPubSubPublisher
import io.micronaut.redis.testcontainers.Redis
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.util.concurrent.TimeUnit

@MicronautTest
class BookPubSubSpec extends Specification implements TestPropertyProvider {

    @Inject
    RedisPubSubPublisher publisher

    @Inject
    BookPublisher bookPublisher

    @Inject
    BookClient bookClient

    @Inject
    BookEventRecorder recorder

    @Override
    Map<String, String> getProperties() {
        Redis.properties
    }

    void "test publisher and client publish JSON events"() {
        when:
        bookPublisher.publish(new BookCreated(title: "Dune", author: "Frank Herbert"))
        bookClient.publish(new BookCreated(title: "Neuromancer", author: "William Gibson"))

        then:
        [recorder.created.poll(10, TimeUnit.SECONDS), recorder.created.poll(10, TimeUnit.SECONDS)] as Set ==
            ["books.created:Dune:Frank Herbert", "books.created:Neuromancer:William Gibson"] as Set
    }

    void "test client publishes plain text"() {
        when:
        bookClient.publishPlain("Foundation")

        then:
        recorder.plain.poll(10, TimeUnit.SECONDS) == "Foundation"
    }

    void "test audit listener is subscribed"() {
        expect: "the returned subscriber count proves that the failing audit listener is subscribed"
        publisher.publish("books.audit", new BookCreated(title: "Dune", author: "Frank Herbert")) >= 1
    }
}
