package example;

import io.micronaut.cache.annotation.Cacheable;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class CachedStringListService {
    private final AtomicInteger invocations = new AtomicInteger();

    @Cacheable("string-lists")
    public List<String> values() {
        return new ArrayList<>(List.of("value-" + invocations.incrementAndGet()));
    }

    public int getInvocationCount() {
        return invocations.get();
    }
}
