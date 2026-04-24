package io.micronaut.configuration.lettuce.pubsub

import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerException
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Consumes
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.redis.test.RedisContainerUtils
import jakarta.inject.Singleton
import org.testcontainers.DockerClientFactory
import spock.lang.AutoCleanup
import spock.util.concurrent.PollingConditions

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

class RedisPubSubSpec extends RedisSpec {

    static final String SPEC_NAME = 'RedisPubSubSpec'

    @AutoCleanup
    ApplicationContext applicationContext

    void "test redis pubsub listener receives channel messages"() {
        given:
        if (!assumeDocker()) {
            return
        }
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
        if (!assumeDocker()) {
            return
        }
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
        if (!assumeDocker()) {
            return
        }
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
        if (!assumeDocker()) {
            return
        }
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

    private static boolean assumeDocker() {
        DockerClientFactory.instance().isDockerAvailable()
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

    @Introspected
    static class Book {
        String title
        String author
    }
}
