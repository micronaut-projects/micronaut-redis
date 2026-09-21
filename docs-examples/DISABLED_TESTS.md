# Python Docs Disabled Test Inventory

This file tracks the Python documentation examples under `docs-examples/codecs-python` and
`docs-examples/pubsub-python` that are present but disabled, reduced, or that carry a workaround because the
direct port of the Java example does not compile or does not behave like the Java example yet. It is the
bug-fixing task list for the Python compiler (`micronaut-inject-python` / `micronaut-context-python`); every row references
a `TODO(python)` comment in the sources.

The Python examples are compiled by every build and their tests run with
`./gradlew pythonCheck -Ppython-ci` (the "Python CI" GitHub workflow), which needs a container runtime for the
Redis test container.

## Migration Rules

- Do not define local copies of Micronaut annotation helpers or custom annotation shims in docs snippets.
  Standard Micronaut annotations are imported from their Java package (`micronaut.configuration.lettuce.pubsub.annotation`,
  `micronaut.messaging.annotation`, `jakarta.inject`, ...).
- Java classes are imported from their package (`from java.util.concurrent import TimeUnit`,
  `from io.lettuce.core.api import StatefulRedisConnection`), never aliased with `java.type(...)`. The imported names work
  in type hints and generic arguments (`StatefulRedisConnection[str, str]`, `AsyncPool[StatefulRedisConnection[str, str]]`,
  `RedisCodec[bytes, bytes]` for `RedisCodec<byte[], byte[]>`).
- A Java `byte[]` is created from a Python `bytes` value with `ByteBuffer.wrap(b"...").array()`
  (`from java.nio import ByteBuffer`): the `RedisCommands<byte[], byte[]>` methods are erased to `Object` parameters,
  so a `bytes` value passed to them directly is not converted.
- `@RedisPubSubClient` interfaces are abstract classes (`ABC`) whose `@MessageChannel` methods are `@abstractmethod`s;
  `@RedisListener` beans are plain classes with `@MessageChannel` methods. Parameter annotations use
  `Annotated[BookCreated, MessageBody]` / `Annotated[str, MessageChannel]`.
- Do not add Java-style getters or setters to Python docs models. Prefer `@Introspected @dataclass` models.
- Methods that implement a Java interface keep the Java (camelCase) name (`handle`); other methods are snake_case
  (`publish_plain`).
- A Python test class is a `@MicronautTest` with injected beans; Python beans are injected like the Java ones
  (`redis_client_commands: Annotated[RedisClientCommands, Inject]` with `from .RedisClientCommands import RedisClientCommands`).
- The `redis.uri` of the shared Redis test container is supplied by the Java `RedisTestConfigurer`
  (`@ContextConfigurer`, `src/test/java`) of each Python project, because Micronaut Test calls a
  `TestPropertyProvider` before the GraalPy runtime exists.

## Active `@Disabled` Tests

None.

## Commented Unsupported Snippet Ports

None.

## Workarounds Kept In Snippets

| Target | Reason |
| --- | --- |
| `io.micronaut.configuration.lettuce.docs.RedisTestConfigurer` (Java, `src/test/java` of both Python projects) | A Python `TestPropertyProvider` test class or `ApplicationContextConfigurer` cannot supply the Redis container address: both run before the GraalPy runtime exists, so the `redis.uri` (and named `redis.servers.*.uri`) properties come from a Java `@ContextConfigurer`. The `testing.adoc` snippet is therefore rendered for Java, Kotlin and Groovy only, with a `[.lang-python]` note. |

## Reduced Ports

None.

## `java.type` Usages

None.

## Intentionally Unsupported Snippet Targets

| Target | Reason |
| --- | --- |
| `io.micronaut.configuration.lettuce.docs.AbstractRedisTest` (`testing.adoc`, `languages="java,kotlin,groovy"`) | A `TestPropertyProvider` cannot be a Python class (see `RedisTestConfigurer` above). |
