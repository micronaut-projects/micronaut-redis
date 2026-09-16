package io.micronaut.configuration.lettuce.docs.pubsub;

import io.micronaut.configuration.lettuce.pubsub.RedisPubSubPublisher;
import io.micronaut.redis.testcontainers.Redis;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BookPubSubTest implements TestPropertyProvider {

    @Inject
    RedisPubSubPublisher publisher;

    @Inject
    BookPublisher bookPublisher;

    @Inject
    BookClient bookClient;

    @Inject
    BookEventRecorder recorder;

    @Override
    public Map<String, String> getProperties() {
        return Redis.getProperties();
    }

    @Test
    void testPublisherAndClientPublishJsonEvents() throws InterruptedException {
        bookPublisher.publish(new BookCreated("Dune", "Frank Herbert"));
        bookClient.publish(new BookCreated("Neuromancer", "William Gibson"));

        assertEquals(
            Set.of("books.created:Dune:Frank Herbert", "books.created:Neuromancer:William Gibson"),
            Set.of(recorder.created.poll(10, TimeUnit.SECONDS), recorder.created.poll(10, TimeUnit.SECONDS))
        );
    }

    @Test
    void testClientPublishesPlainText() throws InterruptedException {
        bookClient.publishPlain("Foundation");

        assertEquals("Foundation", recorder.plain.poll(10, TimeUnit.SECONDS));
    }

    @Test
    void testAuditListenerIsSubscribed() {
        // the returned subscriber count proves that the failing audit listener is subscribed
        assertTrue(publisher.publish("books.audit", new BookCreated("Dune", "Frank Herbert")) >= 1);
    }
}
