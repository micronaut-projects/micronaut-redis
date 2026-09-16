from jakarta.inject import Singleton
from micronaut.configuration.lettuce.pubsub import RedisPubSubPublisher

from .BookCreated import BookCreated


@Singleton
class BookPublisher:

    def __init__(self, publisher: RedisPubSubPublisher):
        self.publisher = publisher

    # tag::publisher[]
    def publish(self, event: BookCreated) -> None:
        self.publisher.publish("books.created", event)
    # end::publisher[]
