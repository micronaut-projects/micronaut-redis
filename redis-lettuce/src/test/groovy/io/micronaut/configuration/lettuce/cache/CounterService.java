package io.micronaut.configuration.lettuce.cache;

import io.micronaut.cache.annotation.*;
import io.micronaut.core.async.annotation.SingleResult;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import jakarta.inject.Singleton;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
@CacheConfig(cacheNames = {"counter"})
public class CounterService {
    Map<String, Integer> counters = new LinkedHashMap<>();
    Map<String, Integer> counters2 = new LinkedHashMap<>();

    public int incrementNoCache(String name) {
        int value = counters.computeIfAbsent(name, (key)-> 0);
        counters.put(name, ++value);
        return value;
    }

    @CachePut
    public int increment(String name) {
        int value = counters.computeIfAbsent(name, (key)-> 0);
        counters.put(name, ++value);
        return value;
    }

    @CachePut("counter")
    @CachePut("counter2")
    public int increment2(String name) {
        int value = counters2.computeIfAbsent(name, (key)-> 0);
        counters2.put(name, ++value);
        return value;
    }

    @Cacheable
    public CompletableFuture<Integer> futureValue(String name) {
        return CompletableFuture.completedFuture(counters.computeIfAbsent(name, (key)-> 0));
    }

    @Cacheable
    @SingleResult
    public Publisher<Integer> flowableValue(String name) {
        return Flux.just(counters.computeIfAbsent(name, (key)-> 0));
    }

    @Cacheable
    public Publisher<Integer> singleValue(String name) {
        return Mono.just(counters.computeIfAbsent(name, (key)-> 0));
    }

    @CachePut
    public CompletableFuture<Integer> futureIncrement(String name) {
        int value = counters.computeIfAbsent(name, (key)-> 0);
        counters.put(name, ++value);
        return CompletableFuture.completedFuture(value);
    }

    @Cacheable
    public int getValue(String name) {
        return counters.computeIfAbsent(name, (key)-> 0);
    }

    @Cacheable("counter2")
    public int getValue2(String name) {
        return counters2.computeIfAbsent(name, (key)-> 0);
    }

    @Cacheable
    public Optional<Integer> getOptionalValue(String name) {
        return Optional.ofNullable(counters.get(name));
    }

    @CacheInvalidate(all = true)
    public void reset() {
        counters.clear();
    }

    @CacheInvalidate
    public void reset(String name) {
        counters.remove(name);
    }

    @CacheInvalidate("counter")
    @CacheInvalidate("counter2")
    public void reset2(String name) {
        counters.remove(name);
    }

    @CacheInvalidate(parameters = "name")
    public void set(String name, int val) {
        counters.put(name, val);
    }
}
