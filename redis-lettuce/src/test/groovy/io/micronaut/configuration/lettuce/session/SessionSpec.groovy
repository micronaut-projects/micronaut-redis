package io.micronaut.configuration.lettuce.session


import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.session.Session
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

@MicronautTest
@Property(name = 'spec.name', value = 'SessionSpec')
@Property(name = "micronaut.session.http.redis.enabled", value = "true")
class SessionSpec extends Specification  implements TestPropertyProvider {

    @Shared
    @AutoCleanup
    GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379)

    @Inject
    @Client("/")
    HttpClient client

    void "test session"() {
        when:
        HttpResponse<String> response = client.toBlocking().exchange(
                HttpRequest.GET("/sessions"), String
        )

        then:
        response.getBody().get() == "not in session"
        response.header(HttpHeaders.SET_COOKIE)

        when:
        String sessionId = response.header(HttpHeaders.SET_COOKIE)
        response = client.toBlocking().exchange(
                HttpRequest.GET("/sessions").header(HttpHeaders.COOKIE, sessionId), String
        )

        then:
        response.getBody().get() == "value in session"
        response.header(HttpHeaders.SET_COOKIE)
    }

    @Override
    Map<String, String> getProperties() {
        redis.start()
        return [
                'redis.uri': 'redis://' + redis.getHost() + ":" + redis.getMappedPort(6379)
        ]
    }

    @Requires(property = 'spec.name', value = 'SessionSpec')
    @Controller("/sessions")
    static class SessionController {

        @Get
        String simple(Session session) {
            return (String) session.get("myValue").orElseGet(() -> {
                session.put("myValue", "value in session");
                return "not in session";
            })
        }

    }
}
