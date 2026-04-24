package io.micronaut.configuration.lettuce.pubsub

import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisPubSubClient
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerException
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import io.micronaut.core.type.Argument
import io.micronaut.core.type.Headers
import io.micronaut.core.type.MutableHeaders
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Consumes
import io.micronaut.http.annotation.Produces
import io.micronaut.http.body.TypedMessageBodyHandler
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.runtime.graceful.GracefulShutdownCapable
import io.micronaut.redis.test.RedisContainerUtils
import jakarta.inject.Singleton
import org.opentest4j.TestAbortedException
import org.testcontainers.DockerClientFactory
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CompletionStage
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class RedisPubSubSpec extends RedisSpec {

    static final String SPEC_NAME = 'RedisPubSubSpec'

    @AutoCleanup
    ApplicationContext applicationContext

    void "test redis pubsub listener receives channel messages"() {
        given:
        assumeDocker()
        applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'spec.name': SPEC_NAME
        )
        RedisPubSubPublisher publisher = applicationContext.getBean(RedisPubSubPublisher)
        ChannelListener listener = applicationContext.getBean(ChannelListener)

        when:
        publisher.publish("books.created", "The Stand")

        then:
        new PollingConditions(timeout: 5).eventually {
            listener.messages.poll() == "books.created:The Stand"
        }
    }

    void "test redis pubsub listener receives pattern messages"() {
        given:
        assumeDocker()
        applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'spec.name': SPEC_NAME
        )
        RedisPubSubPublisher publisher = applicationContext.getBean(RedisPubSubPublisher)
        PatternListener listener = applicationContext.getBean(PatternListener)

        when:
        publisher.publish("books.updated", "Dune")

        then:
        new PollingConditions(timeout: 5).eventually {
            listener.messages.poll() == "books.updated:Dune"
        }
    }

    void "test redis pubsub listener receives richer messages"() {
        given:
        assumeDocker()
        applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'spec.name': SPEC_NAME
        )
        RedisPubSubPublisher publisher = applicationContext.getBean(RedisPubSubPublisher)
        BookListener listener = applicationContext.getBean(BookListener)

        when:
        publisher.publish("books.json", new Book(title: "Dune", author: "Frank Herbert"))

        then:
        new PollingConditions(timeout: 5).eventually {
            listener.messages.poll() == "books.json:Dune:Frank Herbert"
        }
    }

    void "test redis pubsub listener uses per channel exception handlers"() {
        given:
        assumeDocker()
        applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'spec.name': SPEC_NAME
        )
        RedisPubSubPublisher publisher = applicationContext.getBean(RedisPubSubPublisher)
        TitleFailureHandler titleFailureHandler = applicationContext.getBean(TitleFailureHandler)
        AuditFailureHandler auditFailureHandler = applicationContext.getBean(AuditFailureHandler)

        when:
        publisher.publish("books.fail.title", new Book(title: "Dune", author: "Frank Herbert"))
        publisher.publish("books.fail.audit", new Book(title: "The Stand", author: "Stephen King"))

        then:
        new PollingConditions(timeout: 5).eventually {
            titleFailureHandler.failures.poll() == "books.fail.title:Dune"
            auditFailureHandler.failures.poll() == "books.fail.audit:The Stand"
        }
    }

    void "test redis pubsub client publishes richer messages and alternative media types"() {
        given:
        assumeDocker()
        applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'spec.name': SPEC_NAME
        )
        BookClient client = applicationContext.getBean(BookClient)
        ClientJsonListener jsonListener = applicationContext.getBean(ClientJsonListener)
        ClientPlainTextListener plainListener = applicationContext.getBean(ClientPlainTextListener)
        ClientDynamicListener dynamicListener = applicationContext.getBean(ClientDynamicListener)
        ClientPublisherListener publisherListener = applicationContext.getBean(ClientPublisherListener)

        when:
        long subscribers = client.publishCreated(new Book(title: "Dune", author: "Frank Herbert"))
        CompletionStage<Long> plainResult = client.publishPlain(new Book(title: "The Stand", author: "Stephen King"))
        CompletionStage<Void> dynamicResult = client.publishTo("books.client.dynamic", new Book(title: "Foundation", author: "Isaac Asimov"))
        Publisher<Long> publisherResult = client.publishReactive(new Book(title: "Neuromancer", author: "William Gibson"))

        then:
        subscribers == 1
        plainResult.toCompletableFuture().get(5, TimeUnit.SECONDS) == 1
        dynamicResult.toCompletableFuture().get(5, TimeUnit.SECONDS) == null
        Mono.from(publisherResult).block(Duration.ofSeconds(5)) == 1

        and:
        new PollingConditions(timeout: 5).eventually {
            jsonListener.messages.poll() == "books.client.created:Dune:Frank Herbert"
            plainListener.messages.poll() == "books.client.plain:The Stand:Stephen King"
            dynamicListener.messages.poll() == "books.client.dynamic:Foundation:Isaac Asimov"
            publisherListener.messages.poll() == "books.client.publisher:Neuromancer:William Gibson"
        }
    }

    void "test graceful shutdown waits for active redis pubsub listeners"() {
        given:
        assumeDocker()
        applicationContext = ApplicationContext.run(
            'redis.port': RedisContainerUtils.getRedisPort(),
            'spec.name': SPEC_NAME
        )
        RedisPubSubPublisher publisher = applicationContext.getBean(RedisPubSubPublisher)
        GracefulShutdownCapable listenerRegistry = applicationContext.getBean(RedisPubSubListenerRegistry)
        SlowListener listener = applicationContext.getBean(SlowListener)

        when:
        publisher.publish("books.slow", "Dune")

        then:
        listener.started.await(5, TimeUnit.SECONDS)

        when:
        CompletionStage<?> shutdown = listenerRegistry.shutdownGracefully()

        then:
        !shutdown.toCompletableFuture().isDone()
        listenerRegistry.reportActiveTasks().orElseThrow() == 1

        when:
        listener.release.countDown()

        then:
        new PollingConditions(timeout: 5).eventually {
            shutdown.toCompletableFuture().isDone()
            listener.messages.poll() == "books.slow:Dune"
        }
    }

    private static void assumeDocker() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new TestAbortedException("Docker is not available")
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class ChannelListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel("books.created")
        void receive(@MessageBody String body, @MessageChannel String channel) {
            messages.add("${channel}:${body}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class PatternListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel(patterns = "books.*")
        void receive(@MessageBody String body, @MessageChannel String channel) {
            messages.add("${channel}:${body}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class BookListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel("books.json")
        void receive(@MessageBody Book book, @MessageChannel String channel) {
            messages.add("${channel}:${book.title}:${book.author}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class ClientJsonListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel("books.client.created")
        void receive(@MessageBody Book book, @MessageChannel String channel) {
            messages.add("${channel}:${book.title}:${book.author}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class ClientPlainTextListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel("books.client.plain")
        @Consumes(MediaType.TEXT_PLAIN)
        void receive(@MessageBody Book book, @MessageChannel String channel) {
            messages.add("${channel}:${book.title}:${book.author}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class ClientDynamicListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel("books.client.dynamic")
        void receive(@MessageBody Book book, @MessageChannel String channel) {
            messages.add("${channel}:${book.title}:${book.author}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class ClientPublisherListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()

        @MessageChannel("books.client.publisher")
        void receive(@MessageBody Book book, @MessageChannel String channel) {
            messages.add("${channel}:${book.title}:${book.author}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class SlowListener {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>()
        final CountDownLatch started = new CountDownLatch(1)
        final CountDownLatch release = new CountDownLatch(1)

        @MessageChannel("books.slow")
        void receive(@MessageBody String body, @MessageChannel String channel) {
            started.countDown()
            release.await(5, TimeUnit.SECONDS)
            messages.add("${channel}:${body}".toString())
        }
    }

    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class FailingListener {

        @MessageChannel(value = "books.fail.title", exceptionHandler = TitleFailureHandler)
        void receiveTitleFailure(@MessageBody Book book) {
            throw new IllegalStateException(book.title)
        }

        @MessageChannel(value = "books.fail.audit", exceptionHandler = AuditFailureHandler)
        @Consumes(MediaType.APPLICATION_JSON)
        void receiveAuditFailure(@MessageBody Book book) {
            throw new IllegalStateException(book.title)
        }
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class TitleFailureHandler implements RedisListenerExceptionHandler {
        final BlockingQueue<String> failures = new LinkedBlockingQueue<>()

        @Override
        void handle(RedisListenerException exception) {
            failures.add("${exception.messageChannel}:${exception.cause.message}".toString())
        }
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class AuditFailureHandler implements RedisListenerExceptionHandler {
        final BlockingQueue<String> failures = new LinkedBlockingQueue<>()

        @Override
        void handle(RedisListenerException exception) {
            failures.add("${exception.messageChannel}:${exception.cause.message}".toString())
        }
    }

    @RedisPubSubClient
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static interface BookClient {

        @MessageChannel("books.client.created")
        long publishCreated(Book book)

        @MessageChannel("books.client.plain")
        @Produces(MediaType.TEXT_PLAIN)
        CompletionStage<Long> publishPlain(@MessageBody Book book)

        CompletionStage<Void> publishTo(@MessageChannel String channel, Book book)

        @MessageChannel("books.client.publisher")
        Publisher<Long> publishReactive(Book book)
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    static class BookTextPlainBodyHandler implements TypedMessageBodyHandler<Book> {

        @Override
        Argument<Book> getType() {
            return Argument.of(Book)
        }

        @Override
        Book read(Argument<Book> type, MediaType mediaType, Headers httpHeaders, InputStream inputStream) {
            def parts = inputStream.getText('UTF-8').split('\\|', 2)
            return new Book(title: parts[0], author: parts[1])
        }

        @Override
        void writeTo(Argument<Book> type, MediaType mediaType, Book object, MutableHeaders outgoingHeaders, OutputStream outputStream) {
            outputStream.write("${object.title}|${object.author}".getBytes('UTF-8'))
        }
    }

    @Introspected
    static class Book {
        String title
        String author
    }
}
