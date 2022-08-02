package io.micronaut.configuration.lettuce.session

import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.jackson.serialize.JacksonObjectSerializer
import io.micronaut.redis.test.RedisContainerUtils
import io.micronaut.session.Session
import io.micronaut.session.event.AbstractSessionEvent
import io.micronaut.session.event.SessionCreatedEvent
import io.micronaut.session.event.SessionDeletedEvent
import io.micronaut.session.event.SessionExpiredEvent
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoField
import java.time.temporal.ChronoUnit

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class RedisSessionSpec extends RedisSpec {

    void "test redis session create"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'micronaut.session.http.redis.enabled':'true',
                'redis.port': RedisContainerUtils.getRedisPort()
        )
        RedisSessionStore sessionStore = applicationContext.getBean(RedisSessionStore)
        TestListener listener = applicationContext.getBean(TestListener)
        def conditions = new PollingConditions(timeout: 10)

        when:"A new session is created and saved"
        Session session = sessionStore.newSession()
        session.put("username", "fred")
        session.put("foo", new Foo(name: "Fred", age: 10))

        Session saved = sessionStore.save(session).get()

        then:"The session created event is fired and the session is valid"
        conditions.eventually {
            assert listener.events.size() == 1
            assert listener.events.first() instanceof SessionCreatedEvent
        }
        saved != null
        !saved.isExpired()
        saved.maxInactiveInterval
        saved.creationTime
        saved.id
        saved.get("username").get() == "fred"
        saved.get("foo").get() instanceof Foo
        saved.get("foo", Foo).get().name == "Fred"
        saved.get("foo", Foo).get().age == 10

        when:"A session is located"
        listener.events.clear()
        Session  retrieved = sessionStore.findSession(saved.id).get().get()

        then:"Then the session is valid"
        retrieved != null
        !retrieved.isExpired()
        retrieved.maxInactiveInterval
        retrieved.creationTime
        retrieved.id
        retrieved.get("foo", Foo).get() instanceof Foo
        retrieved.get("username", String).get() == "fred"
        retrieved.get("foo", Foo).get().name == "Fred"
        retrieved.get("foo", Foo).get().age == 10

        when:"A session is modified"
        retrieved.remove("username")
        retrieved.put("more", "stuff")
        def now = Instant.now()
        retrieved.setLastAccessedTime(now)
        retrieved.setMaxInactiveInterval(Duration.of(10, ChronoUnit.MINUTES))
        sessionStore.save(retrieved).get()

        retrieved = sessionStore.findSession(retrieved.id).get().get()

        then:"Then the session is valid"
        retrieved != null
        !retrieved.isExpired()
        retrieved.maxInactiveInterval == Duration.of(10, ChronoUnit.MINUTES)
        retrieved.creationTime.getLong(ChronoField.MILLI_OF_SECOND) == saved.creationTime.getLong(ChronoField.MILLI_OF_SECOND)
        retrieved.lastAccessedTime.getLong(ChronoField.MILLI_OF_SECOND) == now.getLong(ChronoField.MILLI_OF_SECOND)
        retrieved.id
        !retrieved.contains("username")
        retrieved.get("more", String).get() == "stuff"
        retrieved.get("foo", Foo).get().name == "Fred"
        retrieved.get("foo", Foo).get().age == 10


        when:"A session is deleted"
        boolean result = sessionStore.deleteSession(saved.id).get()

        then:"A session deleted event is fired"

        result
        conditions.eventually {
            assert listener.events.size() == 1
            assert listener.events.first() instanceof SessionDeletedEvent
        }


        when:"The deleted session is looked up"
        def found = sessionStore.findSession(saved.id).get()

        then:"It is no longer present"
        !found.isPresent()

        cleanup:
        applicationContext.stop()
    }

    void "test redis session expiry"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.redis.enabled':'true'
        )
        RedisSessionStore sessionStore = applicationContext.getBean(RedisSessionStore)
        TestListener listener = applicationContext.getBean(TestListener)
        def conditions = new PollingConditions(timeout: 10)


        when:"A new session is created and saved"
        Session  session = sessionStore.newSession()
        session.put("username", "fred")
        session.put("foo", new Foo(name: "Fred", age: 10))
        session.setMaxInactiveInterval(Duration.ofSeconds(2))
        Session  saved = sessionStore.save(session).get()

        then:
        saved
        conditions.eventually {
            assert listener.events.size() == 2
            assert listener.events[0] instanceof SessionCreatedEvent
            assert listener.events[1] instanceof SessionExpiredEvent
        }

        cleanup:
        applicationContext.stop()
    }

    void "test redis session write behind"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.redis.enabled':'true',
                'micronaut.session.http.redis.writeMode':'background',
        )
        RedisSessionStore sessionStore = applicationContext.getBean(RedisSessionStore)
        def conditions = new PollingConditions(timeout: 10)

        when:"A new session is created and saved"
        Session  session = sessionStore.newSession()
        session.put("username", "fred")
        session.put("foo", new Foo(name: "Fred", age: 10))
        Session  saved = sessionStore.save(session).get()

        then:"The session was saved"
        saved != null

        when:"The session is updated"
        session.put("username","bob")
        session.remove("foo")



        then:"The session was updated in the background"
        conditions.eventually {
            Session retrieved = sessionStore.findSession(session.id).get().get()
            assert !retrieved.contains("foo")
            assert retrieved.get("username", String).get() == 'bob'
        }

        cleanup:
        applicationContext.stop()
    }

    void "test redis JSON sessions"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.redis.valueSerializer':JacksonObjectSerializer.name,
                'micronaut.session.http.redis.enabled':'true'
        )
        RedisSessionStore sessionStore = applicationContext.getBean(RedisSessionStore)
        TestListener listener = applicationContext.getBean(TestListener)
        def conditions = new PollingConditions(timeout: 10)

        when:"A new session is created and saved"
        Session session = sessionStore.newSession()
        session.put("username", "fred")
        session.put("foo", new Foo(name: "Fred", age: 10))

        Session  saved = sessionStore.save(session).get()

        then:"The session created event is fired and the session is valid"
        sessionStore.valueSerializer instanceof JacksonObjectSerializer
        conditions.eventually {
            assert listener.events.size() == 1
            assert listener.events.first() instanceof SessionCreatedEvent
        }
        saved != null
        !saved.isExpired()
        saved.maxInactiveInterval
        saved.creationTime
        saved.id
        saved.get("username").get() == "fred"
        saved.get("foo", Foo).get().name == "Fred"
        saved.get("foo", Foo).get().age == 10

        when:"A session is located"
        listener.events.clear()
        Session  retrieved = sessionStore.findSession(saved.id).get().get()

        then:"Then the session is valid"
        retrieved != null
        !retrieved.isExpired()
        retrieved.maxInactiveInterval
        retrieved.creationTime
        retrieved.id
        retrieved.get("username", String).get() == "fred"
        retrieved.get("foo", Foo).get().name == "Fred"
        retrieved.get("foo", Foo).get().age == 10

        cleanup:
        applicationContext.stop()
    }

    void "test super class properties can be configured"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.cookiePath':'/foo',
                'micronaut.session.http.redis.enabled': true
        )

        expect:
        applicationContext.getBean(RedisHttpSessionConfiguration).getCookiePath().get() == "/foo"

        cleanup:
        applicationContext.stop()
    }

    static class Foo implements Serializable{
        String name
        Integer age
    }

    @Singleton
    static class TestListener implements ApplicationEventListener<AbstractSessionEvent> {
        List<AbstractSessionEvent> events = []
        @Override
        void onApplicationEvent(AbstractSessionEvent event) {
            events.add(event)
        }
    }
}
