package io.micronaut.configuration.lettuce.pubsub.processor

import io.micronaut.configuration.lettuce.pubsub.RedisMessage
import io.micronaut.configuration.lettuce.pubsub.RedisMessageBodyHandler
import io.micronaut.configuration.lettuce.pubsub.RedisPubSubConfiguration
import io.micronaut.configuration.lettuce.pubsub.RedisPubSubListenerRegistry
import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener
import io.micronaut.configuration.lettuce.pubsub.bind.RedisBodyBinder
import io.micronaut.configuration.lettuce.pubsub.bind.RedisBinderRegistry
import io.micronaut.configuration.lettuce.pubsub.bind.RedisChannelBinder
import io.micronaut.configuration.lettuce.pubsub.bind.RedisMessageBinder
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerException
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.convert.ConversionService
import io.micronaut.inject.BeanDefinition
import io.micronaut.http.body.MessageBodyHandlerRegistry
import io.micronaut.messaging.annotation.MessageBody
import jakarta.inject.Singleton
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Consumer

class RedisListenerMethodProcessorSpec extends Specification {

    static final String SPEC_NAME = 'RedisListenerMethodProcessorSpec'

    @AutoCleanup
    ApplicationContext applicationContext = ApplicationContext.run(
        'spec.name': SPEC_NAME
    )

    void "test listener exceptions are delegated to configured handler"() {
        given:
        def beanDefinition = newFailingListenerBeanDefinition()
        def method = beanDefinition.executableMethods.find { it.methodName == 'receive' }
        def conversionService = applicationContext.getBean(ConversionService)
        def bodyHandlerRegistry = applicationContext.getBean(MessageBodyHandlerRegistry)
        def bodyHandler = new RedisMessageBodyHandler(bodyHandlerRegistry, conversionService, new RedisPubSubConfiguration())
        def binderRegistry = new RedisBinderRegistry(
            new RedisBodyBinder(bodyHandler),
            new RedisChannelBinder(conversionService),
            new RedisMessageBinder()
        )
        def listener = new FailingListener()
        def exceptionHandler = new TrackingExceptionHandler()
        applicationContext.registerSingleton(FailingListener, listener)
        applicationContext.registerSingleton(TrackingExceptionHandler, exceptionHandler)
        def listenerRegistry = Mock(RedisPubSubListenerRegistry)
        def processor = new RedisListenerMethodProcessor(applicationContext, binderRegistry, listenerRegistry, bodyHandler)
        Consumer<RedisMessage> consumer

        when:
        processor.process(beanDefinition, method)

        then:
        1 * listenerRegistry.subscribe(_, _, _, _) >> { args ->
            consumer = args[3] as Consumer<RedisMessage>
        }

        when:
        consumer.accept(new RedisMessage("boom".bytes, "books.fail", null))

        then:
        exceptionHandler.exceptions.size() == 1
        with(exceptionHandler.exceptions.first()) {
            cause instanceof IllegalStateException
            messageChannel == 'books.fail'
            redisListener instanceof FailingListener
            redisMessage.map { new String(it.body()) }.orElse(null) == 'boom'
        }
    }

    @SuppressWarnings('unchecked')
    private static BeanDefinition<FailingListener> newFailingListenerBeanDefinition() {
        return (BeanDefinition<FailingListener>) Class
            .forName('io.micronaut.configuration.lettuce.pubsub.processor.$RedisListenerMethodProcessorSpec$FailingListener$Definition')
            .getDeclaredConstructor()
            .newInstance()
    }

    @Singleton
    @RedisListener
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class FailingListener {

        @MessageChannel(value = "books.fail", exceptionHandler = TrackingExceptionHandler)
        void receive(@MessageBody String body) {
            throw new IllegalStateException(body)
        }
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class TrackingExceptionHandler implements RedisListenerExceptionHandler {
        final List<RedisListenerException> exceptions = new CopyOnWriteArrayList<>()

        @Override
        void handle(RedisListenerException exception) {
            exceptions.add(exception)
        }
    }
}
