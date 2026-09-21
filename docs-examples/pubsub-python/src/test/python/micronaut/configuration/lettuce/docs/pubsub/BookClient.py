from abc import ABC, abstractmethod

from micronaut.configuration.lettuce.pubsub.annotation import MessageChannel, RedisPubSubClient
from micronaut.http import MediaType
from micronaut.http.annotation import Produces

from .BookCreated import BookCreated


@RedisPubSubClient
class BookClient(ABC):

    # tag::client[]
    @MessageChannel("books.created")
    @abstractmethod
    def publish(self, event: BookCreated) -> None:
        ...
    # end::client[]

    # tag::clientProduces[]
    @MessageChannel("books.plain-text")
    @Produces(MediaType.TEXT_PLAIN)
    @abstractmethod
    def publish_plain(self, title: str) -> None:
        ...
    # end::clientProduces[]
