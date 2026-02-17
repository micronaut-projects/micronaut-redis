package io.micronaut.redis.testcontainers;

import com.redis.testcontainers.RedisContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class Redis {
    private static final String REDIS_URI = "redis.uri";
    private static final String IMAGE_NAME = RedisContainer.DEFAULT_IMAGE_NAME.asCanonicalNameString();
    private static RedisContainer container;

    public static RedisContainer getContainer() {
        if (container == null) {
            container = new RedisContainer(DockerImageName.parse(IMAGE_NAME))
                .waitingFor(Wait.forListeningPort());;
            container.start();
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while(!container.isRunning());
            return container;
        } else {
            return container;
        }
    }

    public static Map<String, String> getProperties() {
        return getProperties(getContainer());
    }

    private static Map<String, String> getProperties(RedisContainer container) {
        return Map.of(
            REDIS_URI, container.getRedisURI()
        );
    }
}
