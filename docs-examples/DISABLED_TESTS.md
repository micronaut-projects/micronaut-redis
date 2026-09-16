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
- Java classes are imported from their package (`from java.util.concurrent import TimeUnit`), never aliased with
  `java.type(...)`. Lettuce lives in the `io.lettuce` package, which cannot be imported at runtime yet (`io` is a standard
  library module): import its classes inside `try:` (`from io.lettuce.core.api import StatefulRedisConnection`) and fall back
  to the generated `lettuce.*` shim packages in `except ImportError:` (see "Workarounds Kept In Snippets"). The imported
  names work in type hints and generic arguments (`StatefulRedisConnection[str, str]`, `AsyncPool[StatefulRedisConnection[str, str]]`).
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
| `io.micronaut.configuration.lettuce.docs.ByteArrayCodecFactory`, `NamedCodecFactory` (`RedisCodec[bytes, bytes]`) and the `ByteArrayCodecTest` / `NamedCodecTest` tests | `bytes` used as a generic type argument is mapped to `java.lang.Byte` instead of `byte[]` (`RedisCodec<Byte, Byte>`; a top-level `bytes` parameter or return type is mapped to `byte[]` correctly). The bean still returns `ByteArrayCodec.INSTANCE` and the tests inject it with the same (boxed) generic arguments, so they pass, but the bean's type arguments differ from the Java `RedisCodec<byte[], byte[]>` bean. |
| `io.micronaut.configuration.lettuce.docs.JavaBytes` (test helper) | `RedisCommands<byte[], byte[]>` methods are erased to `Object` parameters, so a Python `bytes` value passed to `set`/`get` is not converted to a `byte[]` by GraalPy; the helper builds a `java.type("byte[]")` explicitly (see "`java.type` usages"). |
| Every module importing `io.lettuce.*` types (`RedisClientCommands`, `NamedConnectionCommands`, `PooledRedisCommands`, `ByteArrayCodecFactory`, `NamedCodecFactory` and the codecs tests) | The Python compiler resolves `from io.lettuce.core.api import StatefulRedisConnection` at compile time, but at runtime only `io.micronaut.*` imports are rewritten, so the `io.lettuce` package cannot be imported (`'io' is not a package`); the sources import the generated `lettuce.core.*` shim packages in an `except ImportError` fallback (a star import, `from io.lettuce.core.codec import *`, resolves the factory return type `RedisCodec[bytes, bytes]` to a reflective element that `FactoryBeanElementCreator` rejects: `ReflectClassElement does not support copy constructor`). |
| `io.micronaut.configuration.lettuce.docs.PythonRuntimeInitializer` (Java, `pubsub-python/src/test/java`) | `@MessageChannel` is `@Executable(processOnStartup = true)`, so the Redis listener method processor instantiates the `@RedisListener` beans before the `@Context` GraalPy runtime bean is initialized (`GraalPy context has not been initialized`); the listener creates the GraalPy context bean when an `ExecutableMethodProcessor` is created (same workaround as micronaut-kafka). |
| `io.micronaut.configuration.lettuce.docs.RedisTestConfigurer` (Java, `src/test/java` of both Python projects) | A Python `TestPropertyProvider` test class or `ApplicationContextConfigurer` cannot supply the Redis container address: both run before the GraalPy runtime exists (and default interface methods of Python classes are not bridged to Java), so the `redis.uri` (and named `redis.servers.*.uri`) properties come from a Java `@ContextConfigurer`. The `testing.adoc` snippet is therefore rendered for Java, Kotlin and Groovy only, with a `[.lang-python]` note. |

## Reduced Ports

None.

## `java.type` Usages

| Target | Reason |
| --- | --- |
| `io.micronaut.configuration.lettuce.docs.JavaBytes` (test helper): `java.type("byte[]")` | The primitive array type `byte[]` has no package to import it from; `java.type` is the only way to allocate a Java `byte[]` from Python. |

## Intentionally Unsupported Snippet Targets

| Target | Reason |
| --- | --- |
| `io.micronaut.configuration.lettuce.docs.AbstractRedisTest` (`testing.adoc`, `languages="java,kotlin,groovy"`) | A `TestPropertyProvider` cannot be a Python class (see `RedisTestConfigurer` above). |
