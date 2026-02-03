package example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisTest implements TestPropertyProvider {

    @AfterAll
    static void stopRedis() {
        RedisCluster.stop();
    }

    @Test
    void testRedis(RedisController controller) {
        controller.setKey();
        assertEquals("Hello World", controller.getKey());
        controller.keyCommandSet();
        assertEquals("Hello World", controller.keyCommandGet());
    }

    @Override
    public @NonNull Map<String, String> getProperties() {
        return RedisCluster.getProperties();
    }
}
