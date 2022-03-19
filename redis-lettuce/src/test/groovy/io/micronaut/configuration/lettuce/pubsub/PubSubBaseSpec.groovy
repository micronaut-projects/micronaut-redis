package io.micronaut.configuration.lettuce.pubsub

import io.micronaut.configuration.lettuce.pubsub.executor.RedisConsumerExecutorFactory
import io.micronaut.configuration.lettuce.pubsub.processor.RedisPubSub
import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanLocator
import io.micronaut.context.Qualifier
import io.micronaut.core.convert.DefaultConversionService
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.ApplicationConfiguration
import io.micronaut.scheduling.TaskExecutors
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.ExecutorService

abstract class PubSubBaseSpec extends Specification {

    @AutoCleanup("stop")
    ApplicationContext app

    // Extra connection for sending messages
    RedisPubSub publisher

    public static final String DUMMY_MESSAGE = """ { "message": ["hello", "world"], "timestamp": ${Instant.now().toEpochMilli()} } """
    public static final String DEFAULT_CHANNEL_NAME = "individual_channel"
    public static final MessageChannel DEFAULT_CHANNEL = MessageChannel.individual(DEFAULT_CHANNEL_NAME)

    def setup() {
        app = ApplicationContext.run('redis.type': 'embedded')
        publisher = newPubSubInstance()
    }

    protected <T> T getBean(Class<T> type) {
        return app.getBean(type)
    }

    def publishDummyMessage() {
        publish(DUMMY_MESSAGE)
    }

    def publish(String msg, MessageChannel channel = DEFAULT_CHANNEL) {
        publisher.publish(channel, msg)
    }

    protected RedisPubSub newPubSubInstance(Map props = [:]) {
        ApplicationConfiguration appConfig = new ApplicationConfiguration()
        def executor = app.getBean(ExecutorService, Qualifiers.byName(TaskExecutors.MESSAGE_CONSUMER))

        return new RedisPubSub(executor, new DefaultConversionService(),
                app.getBean(BeanLocator))
    }

}
