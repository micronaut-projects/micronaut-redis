package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.DevelopmentMode
import spock.lang.Specification

/**
 * Outside development mode the factories build the client resources on the calling thread and create plain Lettuce
 * clients, which leave the resources running as they shut down, as before development mode support. In development
 * mode the resources are built on a thread of the library when the context class loader is not the library's, and
 * the client shuts them down with itself.
 */
class ClientResourcesOwnershipSpec extends Specification {

    void "outside development mode the resources are built on the calling thread and the client does not own them"() {
        given:
        ClientResources built = DefaultClientResources.create()
        Thread buildThread = null
        ClientResources.Builder builder = Stub(ClientResources.Builder) {
            build() >> { buildThread = Thread.currentThread(); built }
        }
        ApplicationContext context = ApplicationContext.builder()
                .properties('redis.uri': 'redis://localhost:6379')
                .singletons(Stub(ClientResources) { mutate() >> builder })
                .start()

        when:
        RedisClient client = withForeignContextClassLoader { context.getBean(RedisClient) }

        then:
        buildThread == Thread.currentThread()
        client.getClass() == RedisClient

        when:
        context.stop()

        then:
        !built.eventExecutorGroup().isShuttingDown()

        cleanup:
        built.shutdown()
    }

    void "in development mode the resources are built on a thread of the library and the client owns them"() {
        given:
        ClientResources built = DefaultClientResources.create()
        Thread buildThread = null
        ClientResources.Builder builder = Stub(ClientResources.Builder) {
            build() >> { buildThread = Thread.currentThread(); built }
        }
        ApplicationContext context = ApplicationContext.builder()
                .properties('redis.uri': 'redis://localhost:6379', (DevelopmentMode.PROPERTY): true)
                .singletons(Stub(ClientResources) { mutate() >> builder })
                .start()

        when:
        RedisClient client = withForeignContextClassLoader { context.getBean(RedisClient) }

        then:
        buildThread != Thread.currentThread()
        buildThread.name == 'redis-client-resources'
        client.getClass() != RedisClient

        when:
        context.stop()

        then:
        built.eventExecutorGroup().isShuttingDown()

        cleanup:
        built.shutdown()
    }

    private static <T> T withForeignContextClassLoader(Closure<T> action) {
        Thread thread = Thread.currentThread()
        ClassLoader original = thread.contextClassLoader
        URLClassLoader foreign = new URLClassLoader(new URL[0], original)
        thread.contextClassLoader = foreign
        try {
            return action.call()
        } finally {
            thread.contextClassLoader = original
            foreign.close()
        }
    }
}
