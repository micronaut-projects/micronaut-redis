import time
from typing import Annotated

from jakarta.inject import Inject
from micronaut.configuration.lettuce.pubsub import RedisPubSubPublisher
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .BookClient import BookClient
from .BookCreated import BookCreated
from .BookEventRecorder import BookEventRecorder
from .BookPublisher import BookPublisher


@MicronautTest
class BookPubSubTest:
    publisher: Annotated[RedisPubSubPublisher, Inject]
    book_publisher: Annotated[BookPublisher, Inject]
    book_client: Annotated[BookClient, Inject]
    recorder: Annotated[BookEventRecorder, Inject]

    @Test
    def test_publisher_and_client_publish_json_events(self):
        self.book_publisher.publish(BookCreated("Dune", "Frank Herbert"))
        self.book_client.publish(BookCreated("Neuromancer", "William Gibson"))

        assert self._await(lambda: len(self.recorder.created) == 2)
        assert set(self.recorder.created) == {"books.created:Dune:Frank Herbert", "books.created:Neuromancer:William Gibson"}

    @Test
    def test_client_publishes_plain_text(self):
        self.book_client.publish_plain("Foundation")

        assert self._await(lambda: len(self.recorder.plain) == 1)
        assert self.recorder.plain[0] == "Foundation"

    @Test
    def test_audit_listener_is_subscribed(self):
        # the returned subscriber count proves that the failing audit listener is subscribed
        assert self.publisher.publish("books.audit", BookCreated("Dune", "Frank Herbert")) >= 1

    @staticmethod
    def _await(condition, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.1)
        return condition()
