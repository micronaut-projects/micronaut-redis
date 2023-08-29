package example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

@MicronautTest
class RedisTest {

    @Test
    void testRedis(RedisController controller) {
        controller.setKey();
        assertEquals("Hello World", controller.getKey());
        controller.keyCommandSet();
        assertEquals("Hello World", controller.keyCommandGet());
    }
    
}