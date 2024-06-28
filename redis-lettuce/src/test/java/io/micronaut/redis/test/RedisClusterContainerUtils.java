package io.micronaut.redis.test;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testcontainers.containers.Network.SHARED;

public final class RedisClusterContainerUtils {

    public static final int REDIS_PORT = 6379;
    public static final int TOTAL_NODES = 7;
    private static final DockerImageName REDIS_CLUSTER_DOCKER_NAME = DockerImageName.parse("bitnami/redis-cluster");
    private static final String CREATOR_NODE_NAME = "redis-cluster-creator";
    private static final String REGULAR_NODE_NAME = "redis-cluster-node-%02d";
    private static final Map<String, Integer> redisClusterPortMappings = new HashMap<>();
    private static List<GenericContainer> redisClusterNodes;

    private RedisClusterContainerUtils() {
    }

    public static void close() {
        stopRedisCluster();
    }

    public static List<String> getRedisClusterUris() {
        startRedisCluster();
        return redisClusterPortMappings.values().stream().map(RedisClusterContainerUtils::toRedisUri).toList();
    }

    public static void startRedisCluster() {
        if (redisClusterNodes == null) {
            redisClusterNodes = range(0, TOTAL_NODES).mapToObj(RedisClusterContainerUtils::createContainerNode).toList();
            redisClusterPortMappings.clear();
            for (GenericContainer container : redisClusterNodes) {
                container.start();
                redisClusterPortMappings.put(getIpAddress(container), getMappedPort(container));
            }
        }
    }

    public static void stopRedisCluster() {
        if (redisClusterNodes != null) {
            redisClusterNodes.forEach(GenericContainer::stop);
            redisClusterNodes = null;
        }
    }

    public static void fixRedisURI(RedisURI uri) {
        uri.setPort(redisClusterPortMappings.get(uri.getHost()));
        uri.setHost("localhost");
    }

    private static GenericContainer createContainerNode(int nodeId) {
        if (isClusterCreator(nodeId)) {
            return new GenericContainer<>(REDIS_CLUSTER_DOCKER_NAME)
                .withExposedPorts(REDIS_PORT)
                .withNetwork(SHARED)
                .withNetworkAliases(CREATOR_NODE_NAME)
                .withEnv("ALLOW_EMPTY_PASSWORD", "yes")
                .withEnv("REDIS_CLUSTER_CREATOR", "yes")
                .withEnv("REDIS_NODES", range(0, TOTAL_NODES - 1).mapToObj(RedisClusterContainerUtils::getRegularNodeName).collect(joining(",")))
                .withEnv("REDIS_DISABLE_COMMANDS", "KEYS")
                .waitingFor(Wait.forLogMessage(".*Cluster correctly created*\\n", 1));
        }
        return new GenericContainer<>(REDIS_CLUSTER_DOCKER_NAME)
            .withExposedPorts(REDIS_PORT)
            .withNetwork(SHARED)
            .withNetworkAliases(getRegularNodeName(nodeId))
            .withEnv("ALLOW_EMPTY_PASSWORD", "yes")
            .withEnv("REDIS_CLUSTER_CREATOR", "no")
            .withEnv("REDIS_NODES", CREATOR_NODE_NAME)
            .withEnv("REDIS_DISABLE_COMMANDS", "KEYS")
            .waitingFor(Wait.forLogMessage(".*Setting Redis config file.*\\n", 1));
    }

    private static boolean isClusterCreator(int nodeId) {
        return (nodeId + 1) == TOTAL_NODES;
    }

    private static String getRegularNodeName(int nodeId) {
        return String.format(REGULAR_NODE_NAME, nodeId + 1);
    }

    private static String getIpAddress(GenericContainer container) {
        return container.getContainerInfo().getNetworkSettings().getNetworks().values().stream().findFirst().orElseThrow().getIpAddress();
    }

    private static Integer getMappedPort(GenericContainer container) {
        return container.getMappedPort(REDIS_PORT);
    }

    private static String toRedisUri(Integer port) {
        return "redis://localhost:" + port;
    }
}
