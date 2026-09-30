from dataclasses import dataclass

from micronaut.core.annotation import Introspected


@Introspected
@dataclass
class BookCreated:
    title: str | None = None
    author: str | None = None
