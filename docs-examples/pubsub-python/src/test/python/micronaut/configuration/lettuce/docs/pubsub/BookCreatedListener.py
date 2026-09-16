from typing import Annotated

from micronaut.configuration.lettuce.pubsub.annotation import MessageChannel, RedisListener
from micronaut.messaging.annotation import MessageBody

from .BookCreated import BookCreated


@RedisListener
class BookCreatedListener:

    # tag::listener[]
    @MessageChannel("books.created")
    def receive(self, event: Annotated[BookCreated, MessageBody], channel: Annotated[str, MessageChannel]) -> None:
        print(f"Received {event.title} from {channel}")
    # end::listener[]
