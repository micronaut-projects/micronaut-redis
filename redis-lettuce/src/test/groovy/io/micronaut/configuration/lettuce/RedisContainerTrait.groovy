package io.micronaut.configuration.lettuce

import groovy.transform.CompileStatic
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared

@CompileStatic
trait RedisContainerTrait {

    int REDIS_PORT = 6379
    private String REDIS_DOCKER_NAME = "redis:6.2.6"

    @Shared
    @AutoCleanup
    private static GenericContainer<?> redisContainer

    int getRedisPort() {
        if (redisContainer == null) {
            redisContainer = new GenericContainer<>(DockerImageName.parse(REDIS_DOCKER_NAME))
                    .withExposedPorts(REDIS_PORT)
                    .waitingFor(
                            Wait.forLogMessage(".*Ready to accept connections.*\\n", 1)
                    )
            redisContainer.start()
        }
        redisContainer.getMappedPort(REDIS_PORT)
    }

    String getRedisPort(String prefix) {
        "$prefix:$redisPort"
    }

    void stopRedis() {
        redisContainer.stop()
        redisContainer = null
    }
}