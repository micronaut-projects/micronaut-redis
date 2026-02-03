package example;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class RedisCluster {

    private static final DockerImageName IMAGE_NAME =
        DockerImageName.parse("redis:7.2");

    private static GenericContainer<?> container;

    public static Map<String, String> getProperties() {
        if (container == null) {
            container = new GenericContainer<>(IMAGE_NAME)
                .withExposedPorts(6379)
                .waitingFor(Wait.forListeningPort());
            container.start();
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } while (!container.isRunning());
        }
        return getProperties(container);
    }

    private static Map<String, String> getProperties(GenericContainer<?> container) {
        String uri = "redis://localhost:" + container.getMappedPort(6379);

        final Map<String, String> map = new HashMap<>(Map.of(
            "redis.enabled", "true",
            "redis.uri", uri
        ));
        System.out.println("Redis Cluster Properties = " + map);
        return map;
    }

    public static void stop() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }
}
