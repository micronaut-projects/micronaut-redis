package io.micronaut.configuration.lettuce.docs

import io.micronaut.test.support.TestPropertyProvider
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractRedisTest : TestPropertyProvider {

    // tag::container[]
    companion object {
        const val REDIS_DOCKER_NAME = "redis:6.2.6"
        const val REDIS_PORT = 6379
        val REDIS_CONTAINER: GenericContainer<*> = GenericContainer(DockerImageName.parse(REDIS_DOCKER_NAME))
            .withExposedPorts(REDIS_PORT)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
    }

    override fun getProperties(): Map<String, String> {
        if (!REDIS_CONTAINER.isRunning) {
            REDIS_CONTAINER.start()
        }
        return mapOf("redis.uri" to "redis://${REDIS_CONTAINER.host}:${REDIS_CONTAINER.getMappedPort(REDIS_PORT)}")
    }
    // end::container[]
}
