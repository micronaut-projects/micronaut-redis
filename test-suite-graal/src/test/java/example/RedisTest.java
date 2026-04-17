package example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micronaut.jackson.serialize.JacksonObjectSerializer;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.redis.testcontainers.Redis;
import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.Test;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.TestInstance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisTest implements TestPropertyProvider {

    @Test
    void testRedis(RedisController controller) {
        controller.setKey();
        assertEquals("Hello World", controller.getKey());
        controller.keyCommandSet();
        assertEquals("Hello World", controller.keyCommandGet());
    }

    @Test
    void testCachedArrayList(CachedStringListService cachedStringListService) {
        List<String> first = cachedStringListService.values();
        List<String> second = cachedStringListService.values();

        assertEquals(List.of("value-1"), first);
        assertEquals(first, second);
        assertEquals(1, cachedStringListService.getInvocationCount());
    }

    @Override
    public @NonNull Map<String, String> getProperties() {
        Map<String, String> properties = new LinkedHashMap<>(Redis.getProperties());
        properties.put("redis.caches.string-lists.enabled", "true");
        properties.put("redis.cache.value-serializer", JacksonObjectSerializer.class.getName());
        return properties;
    }
}
