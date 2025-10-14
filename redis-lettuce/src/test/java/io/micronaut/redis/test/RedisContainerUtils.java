package io.micronaut.redis.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public final class RedisContainerUtils {
    public static int REDIS_PORT = 6379;
    private static String REDIS_DOCKER_NAME = "redis:6.2.6";

    private RedisContainerUtils() {
    }

    private static GenericContainer<?> redisContainer;

    public static String getRedisPort(String prefix) {
        return prefix + ":" + getRedisPort();
    }

    public static void close() {
        stopRedis();
    }

    public static int getRedisPort() {
        if (redisContainer == null) {
            redisContainer = new GenericContainer<>(DockerImageName.parse(REDIS_DOCKER_NAME))
                .withExposedPorts(REDIS_PORT)
                .waitingFor(
                    Wait.forLogMessage(".*Ready to accept connections.*\\n", 1)
                )
                .withCommand(
                    "redis-server",
                    "--rename-command",
                    "KEYS",
                    ""
                );
            redisContainer.start();
        }
        return redisContainer.getMappedPort(REDIS_PORT);
    }

    public static void stopRedis() {
        if (redisContainer != null) {
            redisContainer.stop();
            redisContainer = null;
        }
    }
}
