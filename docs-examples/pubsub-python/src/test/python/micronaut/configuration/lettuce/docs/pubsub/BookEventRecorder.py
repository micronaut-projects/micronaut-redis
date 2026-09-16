from typing import Annotated

from micronaut.configuration.lettuce.pubsub.annotation import MessageChannel, RedisListener
from micronaut.http import MediaType
from micronaut.http.annotation import Consumes
from micronaut.messaging.annotation import MessageBody

from .BookCreated import BookCreated


@RedisListener
class BookEventRecorder:
    """Records the messages published by the documented publisher and client, next to the documented listeners."""

    def __init__(self):
        self.created: list[str] = []
        self.plain: list[str] = []

    @MessageChannel("books.created")
    def created_event(self, event: Annotated[BookCreated, MessageBody], channel: Annotated[str, MessageChannel]) -> None:
        self.created.append(f"{channel}:{event.title}:{event.author}")

    @Consumes(MediaType.TEXT_PLAIN)
    @MessageChannel("books.plain-text")
    def plain_event(self, body: Annotated[str, MessageBody]) -> None:
        self.plain.append(body)
