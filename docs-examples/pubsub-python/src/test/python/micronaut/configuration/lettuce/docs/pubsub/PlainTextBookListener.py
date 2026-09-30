from typing import Annotated

from micronaut.configuration.lettuce.pubsub.annotation import MessageChannel, RedisListener
from micronaut.http import MediaType
from micronaut.http.annotation import Consumes
from micronaut.messaging.annotation import MessageBody


@RedisListener
class PlainTextBookListener:

    # tag::listener[]
    @Consumes(MediaType.TEXT_PLAIN)
    @MessageChannel("books.plain-text")
    def receive(self, title: Annotated[str, MessageBody]) -> None:
        print(f"Received plain text title {title}")
    # end::listener[]
