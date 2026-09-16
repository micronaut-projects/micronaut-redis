import sys

from jakarta.inject import Singleton
from micronaut.configuration.lettuce.pubsub.exception import RedisListenerException, RedisListenerExceptionHandler


@Singleton
class BookAuditExceptionHandler(RedisListenerExceptionHandler):

    # tag::handler[]
    def handle(self, exception: RedisListenerException) -> None:
        print(f"Redis listener failure on {exception.getMessageChannel()}: {exception.getMessage()}", file=sys.stderr)
    # end::handler[]
