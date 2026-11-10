package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisCredentials
import io.lettuce.core.RedisCredentialsProvider
import io.lettuce.core.RedisURI
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanRegistration
import io.micronaut.context.env.DevelopmentMode
import io.micronaut.context.reload.BeanRetentionPolicy
import io.micronaut.inject.BeanIdentifier
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy

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

    void "in development mode a client whose credentials provider is a class of the application is not retained"() {
        given:
        ResourceOwningClients clients = new ResourceOwningClients()
        ClientResources resources = DefaultClientResources.create()
        URLClassLoader application = new URLClassLoader(new URL[0], getClass().classLoader)
        InvocationHandler handler = { proxy, method, args -> Mono.just(RedisCredentials.just('user', 'secret')) }
        RedisCredentialsProvider provider = (RedisCredentialsProvider) Proxy.newProxyInstance(application, [RedisCredentialsProvider] as Class[], handler)
        RedisURI withApplicationCredentials = RedisURI.create('redis://localhost:6379')
        withApplicationCredentials.credentialsProvider = provider
        RedisURI withLibraryCredentials = RedisURI.create('redis://user:secret@localhost:6379')

        when:
        RedisClient refused = clients.redisClient(resources, withApplicationCredentials)
        RedisClient refusedForReplica = clients.redisClient(resources, withLibraryCredentials, [withApplicationCredentials])
        RedisClient kept = clients.redisClient(resources, withLibraryCredentials)

        then:
        provider.getClass().classLoader == application
        clients.decide(new BeanRegistration<>(BeanIdentifier.of('refused'), null, refused)) == BeanRetentionPolicy.Decision.REFUSE
        clients.decide(new BeanRegistration<>(BeanIdentifier.of('replica'), null, refusedForReplica)) == BeanRetentionPolicy.Decision.REFUSE
        clients.decide(new BeanRegistration<>(BeanIdentifier.of('kept'), null, kept)) == BeanRetentionPolicy.Decision.ABSTAIN
        clients.decide(new BeanRegistration<>(BeanIdentifier.of('other'), null, 'other')) == BeanRetentionPolicy.Decision.ABSTAIN

        cleanup:
        resources.shutdown()
        application.close()
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
