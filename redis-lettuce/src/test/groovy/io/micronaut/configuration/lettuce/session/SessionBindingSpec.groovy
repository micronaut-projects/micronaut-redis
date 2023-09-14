package io.micronaut.configuration.lettuce.session

import io.micronaut.configuration.lettuce.RedisSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.client.HttpClient
import io.micronaut.redis.test.RedisContainerUtils
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.session.Session
import io.micronaut.session.annotation.SessionValue
import spock.lang.Specification

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class SessionBindingSpec extends RedisSpec {

    void "test bind simple session argument using HTTP header processing"() {
        given:
        ApplicationContext context = ApplicationContext.run([
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.redis.enabled':'true',
                'spec.name': 'SessionBindingSpec'
        ])
        EmbeddedServer embeddedServer = context.getBean(EmbeddedServer).start()
        HttpClient client = context.createBean(HttpClient, embeddedServer.getURL())

        when:
        HttpResponse<String> response = client.toBlocking().exchange(HttpRequest.GET("/sessiontest/simple"), String)

        then:
        response.getBody().get()== "not in session"
        response.header(HttpHeaders.AUTHORIZATION_INFO)

        when:
        def sessionId = response.header(HttpHeaders.AUTHORIZATION_INFO)

        response = client.toBlocking().exchange(
                HttpRequest.GET("/sessiontest/simple")
                .header(HttpHeaders.AUTHORIZATION_INFO, sessionId), String
        )

        then:
        response.getBody().get() == "value in session"
        response.header(HttpHeaders.AUTHORIZATION_INFO)

        cleanup:
        embeddedServer.stop()
    }


    void "test bind simple session argument using Cookie processing"() {
        given:
        ApplicationContext context = ApplicationContext.run([
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.redis.enabled':'true',
                'spec.name': 'SessionBindingSpec'
        ])
        EmbeddedServer embeddedServer = context.getBean(EmbeddedServer).start()
        HttpClient client = context.createBean(HttpClient, embeddedServer.getURL())

        when:
        HttpResponse<String> response = client.toBlocking().exchange(
                HttpRequest.GET("/sessiontest/simple"), String
        )

        then:
        response.getBody().get() == "not in session"
        response.header(HttpHeaders.SET_COOKIE)

        when:
        def sessionId = response.header(HttpHeaders.SET_COOKIE)

        response = client.toBlocking().exchange(
                HttpRequest.GET("/sessiontest/simple")
                .header(HttpHeaders.COOKIE, sessionId), String
        )

        then:
        response.getBody().get() == "value in session"
        response.header(HttpHeaders.SET_COOKIE)

        cleanup:
        embeddedServer.stop()
    }

    void "test bind optional session"() {
        given:
        ApplicationContext context = ApplicationContext.run([
                'redis.port': RedisContainerUtils.getRedisPort(),
                'micronaut.session.http.redis.enabled':'true',
                'spec.name': 'SessionBindingSpec'
        ])
        EmbeddedServer embeddedServer = context.getBean(EmbeddedServer).start()
        HttpClient client = context.createBean(HttpClient, embeddedServer.getURL())

        when:
        HttpResponse<String> response = client.toBlocking().exchange(
                HttpRequest.GET("/sessiontest/optional"), String
        )

        then:
        response.getBody().get() == "no session"
        !response.header(HttpHeaders.AUTHORIZATION_INFO)

        when:
        response = client.toBlocking().exchange(
                HttpRequest.GET("/sessiontest/simple"), String
        )

        then:
        response.getBody().get() == "not in session"
        response.header(HttpHeaders.AUTHORIZATION_INFO)

        when:
        def sessionId = response.header(HttpHeaders.AUTHORIZATION_INFO)

        response = client.toBlocking().exchange(
                HttpRequest.GET("/sessiontest/optional")
                        .header(HttpHeaders.AUTHORIZATION_INFO, sessionId), String
        )

        then:
        response.getBody().get() == "value in session"
        response.header(HttpHeaders.AUTHORIZATION_INFO)

        cleanup:
        embeddedServer.stop()
    }

    @Requires(property = 'spec.name', value = 'SessionBindingSpec')
    @Controller('/sessiontest')
    static class SessionController {

        @Get("/simple")
        String simple(Session session) {
            return session.get("myValue", String).orElseGet({
                session.put("myValue", "value in session")
                "not in session"
            })
        }

        @Get("/value")
        String value(@SessionValue Optional<String> myValue) {
            return myValue.orElse(
                    "no value in session"
            )
        }

        @Get("/optional")
        String optional(Optional<Session> session) {
            if(session.isPresent()) {
                def s = session.get()
                return s.get("myValue", String).orElseGet({
                    s.put("myValue", "value in session")
                    "not in session"
                })
            }
            else {
                return "no session"
            }
        }
    }
}
