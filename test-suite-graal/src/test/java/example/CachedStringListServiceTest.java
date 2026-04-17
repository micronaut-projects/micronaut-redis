package example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class CachedStringListServiceTest {

    @Test
    void valuesAndInvocationCountIncrementWithoutCacheInterceptor() {
        CachedStringListService service = new CachedStringListService();

        assertEquals(List.of("value-1"), service.values());
        assertEquals(List.of("value-2"), service.values());
        assertEquals(2, service.getInvocationCount());
    }
}
