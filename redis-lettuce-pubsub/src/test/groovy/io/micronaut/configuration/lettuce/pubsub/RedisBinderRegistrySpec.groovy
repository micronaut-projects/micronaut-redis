package io.micronaut.configuration.lettuce.pubsub

import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.bind.RedisBinderRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Executable
import io.micronaut.context.annotation.Requires
import io.micronaut.core.bind.DefaultExecutableBinder
import io.micronaut.core.annotation.Introspected
import io.micronaut.http.MediaType
import io.micronaut.messaging.annotation.MessageBody
import jakarta.inject.Singleton
import spock.lang.AutoCleanup
import spock.lang.Specification

class RedisBinderRegistrySpec extends Specification {

    static final String SPEC_NAME = 'RedisBinderRegistrySpec'

    @AutoCleanup
    ApplicationContext applicationContext = ApplicationContext.run(
        'redis.port': -1,
        'spec.name': SPEC_NAME
    )

    void "test redis listener arguments are bound from message body and channel"() {
        given:
        def target = applicationContext.getBean(BindingTarget)
        def method = applicationContext.getBeanDefinition(BindingTarget).executableMethods.find { it.methodName == 'receive' }
        def registry = applicationContext.getBean(RedisBinderRegistry)
        def binder = new DefaultExecutableBinder<RedisListenerMessage>()

        when:
        binder.bind(method, registry, new RedisListenerMessage(
            new RedisMessage("Dune".bytes, "books.created", "books.*"),
            MediaType.TEXT_PLAIN_TYPE
        )).invoke(target)

        then:
        target.body == "Dune"
        target.channel == "books.created"
        target.message.getPattern().orElse(null) == "books.*"
    }

    void "test redis listener binds richer message bodies with json by default"() {
        given:
        def target = applicationContext.getBean(RichBindingTarget)
        def method = applicationContext.getBeanDefinition(RichBindingTarget).executableMethods.find { it.methodName == 'receive' }
        def registry = applicationContext.getBean(RedisBinderRegistry)
        def bodyHandler = applicationContext.getBean(RedisMessageBodyHandler)
        def binder = new DefaultExecutableBinder<RedisListenerMessage>()
        def payload = new Book(title: "Dune", author: "Frank Herbert")
        def bytes = bodyHandler.serialize(payload)

        when:
        binder.bind(method, registry, new RedisListenerMessage(
            new RedisMessage(bytes, "books.created", null),
            MediaType.APPLICATION_JSON_TYPE
        )).invoke(target)

        then:
        target.book.title == "Dune"
        target.book.author == "Frank Herbert"
        target.channel == "books.created"
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class BindingTarget {
        String body
        String channel
        RedisMessage message

        @Executable
        void receive(@MessageBody String body, @MessageChannel String channel, RedisMessage message) {
            this.body = body
            this.channel = channel
            this.message = message
        }
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class RichBindingTarget {
        Book book
        String channel

        @Executable
        void receive(@MessageBody Book book, @MessageChannel String channel) {
            this.book = book
            this.channel = channel
        }
    }

    @Introspected
    static class Book {
        String title
        String author
    }
}
