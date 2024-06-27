package io.micronaut.redis.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testcontainers.containers.Network.SHARED;

public final class RedisReplicaContainerUtils {
    public static int REDIS_PORT = 6379;
    private static String REDIS_DOCKER_NAME = "redis:6.2.6";
    private static final String PRIMARY_NODE_NAME = "redis-primary";
    private static final String REPLICA_NODE_NAME = "redis-replica";

    private static final Map<String, Integer> redisReplicaPortMappings = new HashMap<>();

    private RedisReplicaContainerUtils() {
    }

    private static GenericContainer<?> primaryContainer;
    private static GenericContainer<?> replicaContainer;

    public static String getRedisPrimaryUri() {
        int port = startRedis();

        return toRedisUri(port);
    }

    public static void close() {
        stopRedis();
    }

    public static List<String> getRedisReplicaUris() {
        startRedis();
        return redisReplicaPortMappings.values().stream().map(RedisReplicaContainerUtils::toRedisUri).toList();
    }

    public static int startRedis() {
        if (primaryContainer == null) {
            primaryContainer = new GenericContainer<>(DockerImageName.parse(REDIS_DOCKER_NAME))
                .withExposedPorts(REDIS_PORT)
                .withNetwork(SHARED)
                .withNetworkAliases(PRIMARY_NODE_NAME)
                .waitingFor(Wait.forListeningPort());
            primaryContainer.start();
        }

        if (replicaContainer == null) {
            replicaContainer = new GenericContainer<>(DockerImageName.parse(REDIS_DOCKER_NAME))
                .withExposedPorts(REDIS_PORT)
                .withNetwork(SHARED)
                .withNetworkAliases(REPLICA_NODE_NAME)
                .withCommand(
                    "redis-server",
                    "--slaveof",
                    PRIMARY_NODE_NAME,
                    String.valueOf(REDIS_PORT)
                )
                .waitingFor(Wait.forListeningPort());
            replicaContainer.start();

            redisReplicaPortMappings.put(REPLICA_NODE_NAME, replicaContainer.getMappedPort(REDIS_PORT));
        }
        return primaryContainer.getMappedPort(REDIS_PORT);
    }

    private static String toRedisUri(Integer port) {
        return "redis://localhost:" + port;
    }

    public static void stopRedis() {
        if (primaryContainer != null) {
            primaryContainer.stop();
            primaryContainer = null;
        }
        if (replicaContainer != null) {
            replicaContainer.stop();
            replicaContainer = null;
        }
    }
}
