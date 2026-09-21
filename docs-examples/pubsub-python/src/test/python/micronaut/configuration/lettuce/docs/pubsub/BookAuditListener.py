from typing import Annotated

from micronaut.configuration.lettuce.pubsub.annotation import MessageChannel, RedisListener
from micronaut.messaging.annotation import MessageBody

from .BookAuditExceptionHandler import BookAuditExceptionHandler
from .BookCreated import BookCreated


@RedisListener
class BookAuditListener:

    # tag::listener[]
    @MessageChannel(value="books.audit", exceptionHandler=BookAuditExceptionHandler)
    def receive(self, event: Annotated[BookCreated, MessageBody]) -> None:
        raise RuntimeError(f"Could not audit {event.title}")
    # end::listener[]
