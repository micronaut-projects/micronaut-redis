package io.micronaut.configuration.lettuce.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

abstract class AbstractRedisTest extends Specification implements TestPropertyProvider {

    // tag::container[]
    static final String REDIS_DOCKER_NAME = "redis:6.2.6"
    static final int REDIS_PORT = 6379
    static final GenericContainer<?> REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse(REDIS_DOCKER_NAME))
        .withExposedPorts(REDIS_PORT)
        .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))

    @Override
    Map<String, String> getProperties() {
        if (!REDIS_CONTAINER.running) {
            REDIS_CONTAINER.start()
        }
        ['redis.uri': "redis://${REDIS_CONTAINER.host}:${REDIS_CONTAINER.getMappedPort(REDIS_PORT)}".toString()]
    }
    // end::container[]
}
