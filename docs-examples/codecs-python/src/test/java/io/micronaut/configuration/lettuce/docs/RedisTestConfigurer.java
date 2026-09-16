package io.micronaut.configuration.lettuce.docs;

import io.micronaut.context.ApplicationContextBuilder;
import io.micronaut.context.ApplicationContextConfigurer;
import io.micronaut.context.annotation.ContextConfigurer;
import io.micronaut.redis.testcontainers.Redis;

import java.util.Map;

/**
 * Points every test application context of this project at the shared Redis test container.
 * <p>
 * The configurer is written in Java because the properties are needed before the GraalPy
 * runtime of the application exists, so neither a Python {@code TestPropertyProvider} test
 * class nor a Python {@code ApplicationContextConfigurer} can supply them.
 */
@ContextConfigurer
public class RedisTestConfigurer implements ApplicationContextConfigurer {

    @Override
    public void configure(ApplicationContextBuilder builder) {
        String uri = Redis.getContainer().getRedisURI();
        builder.properties(Map.of(
            "redis.uri", uri,
            "redis.servers.foo.uri", uri,
            "redis.servers.bar.uri", uri
        ));
    }
}
